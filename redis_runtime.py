import atexit
import hashlib
import json
import os
import shutil
import socket
import ssl
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis
from redis.exceptions import RedisError


class RedisNotConfiguredError(RedisError):
    """Raised when Redis is required but connection settings are missing."""


_LOCK = threading.RLock()
_TEXT_CLIENT: Optional[redis.Redis] = None
_BLOB_CLIENT: Optional[redis.Redis] = None
_CONFIG_CACHE: Optional[Dict[str, Any]] = None
_CONFIG_SOURCE = "unknown"
_CONFIG_ERROR = ""
_CONFIG_FALLBACK_REASON = ""
_RUNTIME_DIR = (Path(__file__).resolve().parent / ".runtime").resolve()
_INTERNAL_REDIS_PROCESS: Optional[subprocess.Popen] = None
_INTERNAL_REDIS_INFO: Dict[str, Any] = {}
_INTERNAL_REDIS_ATEXIT_REGISTERED = False
_REDIS_ENCRYPTION_KEY_PATH = (_RUNTIME_DIR / "redis_encryption.key").resolve()
_REDIS_LIVE_ENCRYPTION_STATE_PATH = (_RUNTIME_DIR / "redis_live_encryption.json").resolve()
_REDIS_ENCRYPTED_SNAPSHOT_PATH = (_RUNTIME_DIR / "redis_snapshot.enc").resolve()  # legacy artifact path

_LIVE_ENCRYPTION_STATE_CACHE: Optional[Dict[str, Any]] = None

_REDIS_ENCRYPTION_PREFIX_TEXT = "enc:v1:"
_REDIS_ENCRYPTION_PREFIX_BYTES = _REDIS_ENCRYPTION_PREFIX_TEXT.encode("ascii")
_REDIS_MODE_INTERNAL = "internal"
_REDIS_MODE_EXTERNAL = "external"

# Counter keys must remain plaintext for atomic INCR/INCRBY operations.
_NUMERIC_PLAINTEXT_KEY_PREFIXES = (
    "tater:hydra:metrics:",
    "tater:conversation_artifact_seq:",
)


def _safe_chmod(path: Path, mode: int = 0o600) -> None:
    try:
        os.chmod(path, mode)
    except Exception:
        pass


def _load_fernet_primitives():
    try:
        from cryptography.fernet import Fernet, InvalidToken

        return Fernet, InvalidToken
    except Exception as exc:  # pragma: no cover - import guard for optional dependency
        raise RuntimeError("Redis encryption requires the `cryptography` package.") from exc


def _redis_encryption_key_fingerprint(key_bytes: bytes) -> str:
    try:
        return hashlib.sha256(bytes(key_bytes)).hexdigest()[:16]
    except Exception:
        return ""


def _read_redis_encryption_key(*, create_if_missing: bool = False) -> Tuple[bytes, bool]:
    Fernet, _InvalidToken = _load_fernet_primitives()
    key_path = _REDIS_ENCRYPTION_KEY_PATH
    if key_path.exists():
        key_bytes = key_path.read_bytes().strip()
        if not key_bytes:
            raise RuntimeError(f"Redis encryption key is empty: {key_path}")
        try:
            Fernet(key_bytes)
        except Exception as exc:
            raise RuntimeError(f"Redis encryption key is invalid: {key_path}") from exc
        return key_bytes, False

    if not create_if_missing:
        raise FileNotFoundError(f"Redis encryption key not found: {key_path}")

    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_bytes = Fernet.generate_key()
    tmp_path = key_path.with_suffix(key_path.suffix + ".tmp")
    tmp_path.write_bytes(key_bytes + b"\n")
    _safe_chmod(tmp_path)
    tmp_path.replace(key_path)
    _safe_chmod(key_path)
    return key_bytes, True


def _live_encryption_state_path() -> Path:
    raw = str(os.getenv("TATER_REDIS_LIVE_ENCRYPTION_STATE_PATH", "") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (Path(__file__).resolve().parent / path).resolve()
        return path
    return _REDIS_LIVE_ENCRYPTION_STATE_PATH


def _live_encryption_state_default() -> Dict[str, Any]:
    return {
        "enabled": False,
        "updated_at_epoch": 0,
        "updated_at": "",
    }


def _load_live_encryption_state_locked() -> Dict[str, Any]:
    global _LIVE_ENCRYPTION_STATE_CACHE
    if isinstance(_LIVE_ENCRYPTION_STATE_CACHE, dict):
        return dict(_LIVE_ENCRYPTION_STATE_CACHE)

    path = _live_encryption_state_path()
    state = _live_encryption_state_default()
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                state["enabled"] = bool(raw.get("enabled"))
                state["updated_at_epoch"] = _to_int(raw.get("updated_at_epoch"), default=0, min_value=0)
                state["updated_at"] = str(raw.get("updated_at") or "")
        except Exception:
            pass

    _LIVE_ENCRYPTION_STATE_CACHE = dict(state)
    return dict(state)


def _save_live_encryption_state_locked(enabled: bool) -> Dict[str, Any]:
    global _LIVE_ENCRYPTION_STATE_CACHE
    now_epoch = int(time.time())
    state = {
        "enabled": bool(enabled),
        "updated_at_epoch": now_epoch,
        "updated_at": datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat(),
    }
    path = _live_encryption_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    _safe_chmod(tmp_path)
    tmp_path.replace(path)
    _safe_chmod(path)
    _LIVE_ENCRYPTION_STATE_CACHE = dict(state)
    return dict(state)


def _live_encryption_enabled() -> bool:
    with _LOCK:
        state = _load_live_encryption_state_locked()
    return bool(state.get("enabled"))


def _key_requires_plaintext_counter(key: Any) -> bool:
    if isinstance(key, (bytes, bytearray, memoryview)):
        key_text = bytes(key).decode("utf-8", errors="ignore")
    else:
        key_text = str(key or "")
    for prefix in _NUMERIC_PLAINTEXT_KEY_PREFIXES:
        if key_text.startswith(prefix):
            return True
    return False


def _value_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


def _is_encrypted_value(value: Any) -> bool:
    if isinstance(value, str):
        return value.startswith(_REDIS_ENCRYPTION_PREFIX_TEXT)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).startswith(_REDIS_ENCRYPTION_PREFIX_BYTES)
    return False


def _encrypt_value(value: Any, *, decode_responses: bool) -> Any:
    if value is None:
        return value
    if _is_encrypted_value(value):
        return value

    Fernet, _InvalidToken = _load_fernet_primitives()
    key_bytes, _created = _read_redis_encryption_key(create_if_missing=True)
    token = Fernet(key_bytes).encrypt(_value_bytes(value))
    payload = _REDIS_ENCRYPTION_PREFIX_BYTES + token
    if decode_responses:
        return payload.decode("ascii")
    return payload


def _decrypt_value(value: Any, *, decode_responses: bool) -> Any:
    if value is None:
        return None

    token_bytes: bytes
    if isinstance(value, str):
        if not value.startswith(_REDIS_ENCRYPTION_PREFIX_TEXT):
            return value
        token_bytes = value[len(_REDIS_ENCRYPTION_PREFIX_TEXT) :].encode("ascii", errors="ignore")
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if not raw.startswith(_REDIS_ENCRYPTION_PREFIX_BYTES):
            return value
        token_bytes = raw[len(_REDIS_ENCRYPTION_PREFIX_BYTES) :]
    else:
        return value

    try:
        Fernet, _InvalidToken = _load_fernet_primitives()
        key_bytes, _created = _read_redis_encryption_key(create_if_missing=False)
        decoded = Fernet(key_bytes).decrypt(token_bytes)
    except Exception:
        return value

    if decode_responses:
        return decoded.decode("utf-8", errors="replace")
    return decoded


def _redis_type_token(raw_type: Any) -> str:
    if isinstance(raw_type, bytes):
        return raw_type.decode("utf-8", errors="ignore").strip().lower()
    return str(raw_type or "").strip().lower()


def _apply_key_ttl_ms(client: redis.Redis, key: bytes, ttl_ms: int) -> None:
    try:
        ttl_value = int(ttl_ms)
    except Exception:
        return
    if ttl_value >= 0:
        client.pexpire(key, ttl_value)


def _transform_key_values(client: redis.Redis, key: bytes, *, encrypt_mode: bool) -> Dict[str, int]:
    summary = {"values_changed": 0, "key_changed": 0}
    key_type = _redis_type_token(client.type(key))

    if key_type == "string":
        if _key_requires_plaintext_counter(key):
            return summary
        raw_value = client.get(key)
        if raw_value is None:
            return summary
        next_value = _encrypt_value(raw_value, decode_responses=False) if encrypt_mode else _decrypt_value(raw_value, decode_responses=False)
        if next_value != raw_value:
            ttl_ms = int(client.pttl(key))
            client.set(key, next_value)
            _apply_key_ttl_ms(client, key, ttl_ms)
            summary["values_changed"] = 1
            summary["key_changed"] = 1
        return summary

    if key_type == "hash":
        mapping = client.hgetall(key) or {}
        changed: Dict[bytes, bytes] = {}
        for field, raw_value in mapping.items():
            next_value = _encrypt_value(raw_value, decode_responses=False) if encrypt_mode else _decrypt_value(raw_value, decode_responses=False)
            if next_value != raw_value:
                changed[bytes(field)] = bytes(next_value)
        if changed:
            client.hset(key, mapping=changed)
            summary["values_changed"] = len(changed)
            summary["key_changed"] = 1
        return summary

    if key_type == "list":
        items = list(client.lrange(key, 0, -1) or [])
        if not items:
            return summary
        rebuilt = []
        changed = 0
        for raw_value in items:
            next_value = _encrypt_value(raw_value, decode_responses=False) if encrypt_mode else _decrypt_value(raw_value, decode_responses=False)
            rebuilt.append(next_value)
            if next_value != raw_value:
                changed += 1
        if changed:
            ttl_ms = int(client.pttl(key))
            client.delete(key)
            client.rpush(key, *rebuilt)
            _apply_key_ttl_ms(client, key, ttl_ms)
            summary["values_changed"] = changed
            summary["key_changed"] = 1
        return summary

    if key_type == "set":
        members = list(client.smembers(key) or set())
        if not members:
            return summary
        rebuilt = []
        changed = 0
        for raw_value in members:
            # Set members are identity values; keep them plaintext so membership/removal
            # operations remain stable (legacy encrypted members are normalized here).
            next_value = _decrypt_value(raw_value, decode_responses=False)
            rebuilt.append(next_value)
            if next_value != raw_value:
                changed += 1
        if changed:
            ttl_ms = int(client.pttl(key))
            client.delete(key)
            if rebuilt:
                client.sadd(key, *rebuilt)
            _apply_key_ttl_ms(client, key, ttl_ms)
            summary["values_changed"] = changed
            summary["key_changed"] = 1
        return summary

    if key_type == "zset":
        rows = list(client.zrange(key, 0, -1, withscores=True) or [])
        if not rows:
            return summary
        rebuilt: Dict[bytes, float] = {}
        changed = 0
        for raw_member, score in rows:
            # ZSET members are identity values; keep them plaintext so zadd/zrem/zscore/
            # zincrby stay deterministic (legacy encrypted members are normalized here).
            next_member = _decrypt_value(raw_member, decode_responses=False)
            if next_member != raw_member:
                changed += 1
            rebuilt[bytes(next_member)] = float(score)
        if changed:
            ttl_ms = int(client.pttl(key))
            client.delete(key)
            if rebuilt:
                client.zadd(key, rebuilt)
            _apply_key_ttl_ms(client, key, ttl_ms)
            summary["values_changed"] = changed
            summary["key_changed"] = 1
        return summary

    # unsupported/opaque types (stream, module, etc.): leave untouched
    return summary


def _transform_live_redis_values(*, encrypt_mode: bool) -> Dict[str, int]:
    client = get_redis_client(decode_responses=False)
    keys_scanned = 0
    keys_changed = 0
    values_changed = 0

    for raw_key in client.scan_iter(match=b"*", count=500):
        if raw_key is None:
            continue
        key = bytes(raw_key)
        keys_scanned += 1
        row = _transform_key_values(client, key, encrypt_mode=encrypt_mode)
        values_changed += int(row.get("values_changed") or 0)
        if int(row.get("key_changed") or 0) > 0:
            keys_changed += 1

    return {
        "keys_scanned": int(keys_scanned),
        "keys_changed": int(keys_changed),
        "values_changed": int(values_changed),
    }


def ensure_redis_encryption_key() -> Dict[str, Any]:
    key_bytes, key_created = _read_redis_encryption_key(create_if_missing=True)
    return {
        "key_created": bool(key_created),
        "key_exists": True,
        "key_path": str(_REDIS_ENCRYPTION_KEY_PATH),
        "key_fingerprint": _redis_encryption_key_fingerprint(key_bytes),
    }


def get_redis_encryption_status() -> Dict[str, Any]:
    with _LOCK:
        state = _load_live_encryption_state_locked()
    status: Dict[str, Any] = {
        "encryption_available": True,
        "key_exists": False,
        "key_path": str(_REDIS_ENCRYPTION_KEY_PATH),
        "key_fingerprint": "",
        "live_encryption_enabled": bool(state.get("enabled")),
        "live_encryption_state_path": str(_live_encryption_state_path()),
        "live_encryption_updated": str(state.get("updated_at") or ""),
        # Backward-compatible fields used by prior UI.
        "snapshot_exists": bool(state.get("enabled")),
        "snapshot_path": str(_live_encryption_state_path()),
        "snapshot_size_bytes": 0,
        "snapshot_modified": str(state.get("updated_at") or ""),
        "error": "",
    }

    try:
        _load_fernet_primitives()
    except Exception as exc:
        status["encryption_available"] = False
        status["error"] = str(exc)
        return status

    try:
        key_bytes, _created = _read_redis_encryption_key(create_if_missing=False)
        status["key_exists"] = True
        status["key_fingerprint"] = _redis_encryption_key_fingerprint(key_bytes)
    except FileNotFoundError:
        status["key_exists"] = False
    except Exception as exc:
        status["error"] = str(exc)

    return status


def encrypt_current_redis_snapshot() -> Dict[str, Any]:
    # Legacy function name kept for API compatibility. Behavior now performs
    # in-place live-value encryption and enables live encryption mode.
    key_bytes, key_created = _read_redis_encryption_key(create_if_missing=True)
    transform_summary = _transform_live_redis_values(encrypt_mode=True)
    with _LOCK:
        _save_live_encryption_state_locked(True)
    return {
        "encrypted": True,
        "key_created": bool(key_created),
        "key_path": str(_REDIS_ENCRYPTION_KEY_PATH),
        "key_fingerprint": _redis_encryption_key_fingerprint(key_bytes),
        "keys_encrypted": int(transform_summary.get("values_changed") or 0),
        "keys_changed": int(transform_summary.get("keys_changed") or 0),
        "keys_scanned": int(transform_summary.get("keys_scanned") or 0),
        "live_encryption_enabled": True,
        "created_at_epoch": int(time.time()),
    }


def decrypt_current_redis_snapshot(*, flush_before_restore: bool = True) -> Dict[str, Any]:
    # Legacy function name kept for API compatibility. Behavior now performs
    # in-place live-value decryption and disables live encryption mode.
    _ = bool(flush_before_restore)  # retained for API compatibility
    key_bytes, _created = _read_redis_encryption_key(create_if_missing=False)
    transform_summary = _transform_live_redis_values(encrypt_mode=False)
    with _LOCK:
        _save_live_encryption_state_locked(False)
    return {
        "decrypted": True,
        "restored_keys": int(transform_summary.get("values_changed") or 0),
        "keys_changed": int(transform_summary.get("keys_changed") or 0),
        "keys_scanned": int(transform_summary.get("keys_scanned") or 0),
        "live_encryption_enabled": False,
        "key_path": str(_REDIS_ENCRYPTION_KEY_PATH),
        "snapshot_path": str(_live_encryption_state_path()),
        "key_fingerprint": _redis_encryption_key_fingerprint(key_bytes),
    }


def _config_path() -> Path:
    raw = str(os.getenv("TATER_REDIS_CONFIG_PATH", "") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (Path(__file__).resolve().parent / path).resolve()
        return path
    return (Path(__file__).resolve().parent / ".runtime" / "redis_connection.json").resolve()


def _agent_lab_dir() -> Path:
    raw = str(os.getenv("TATER_AGENT_ROOT", "") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (Path(__file__).resolve().parent / path).resolve()
        return path.resolve()
    return (Path(__file__).resolve().parent / "agent_lab").resolve()


def _default_internal_redis_data_path() -> Path:
    return (_agent_lab_dir() / "redis" / "dump.rdb").resolve()


def _normalize_redis_mode(value: Any, *, default: str = _REDIS_MODE_INTERNAL) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token in {"", "auto", "embedded", "embed", "builtin", "built_in", "local", "redislite", "python"}:
        token = str(default or _REDIS_MODE_INTERNAL).strip().lower()
    if token in {"internal", "embedded", "builtin", "built_in", "local", "redislite", "python"}:
        return _REDIS_MODE_INTERNAL
    if token in {"external", "remote", "server", "tcp", "standalone"}:
        return _REDIS_MODE_EXTERNAL
    return _REDIS_MODE_INTERNAL if str(default or "").strip().lower() == _REDIS_MODE_INTERNAL else _REDIS_MODE_EXTERNAL


def _resolve_internal_redis_data_path(value: Any = None) -> Path:
    raw = str(os.getenv("TATER_REDIS_DATA_PATH", "") or value or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            parts = path.parts
            if parts and parts[0] == "agent_lab":
                path = (Path(__file__).resolve().parent / path).resolve()
            else:
                path = (_agent_lab_dir() / path).resolve()
        elif str(path).startswith("/agent_lab/"):
            path = (_agent_lab_dir() / str(path)[len("/agent_lab/") :]).resolve()
        elif str(path) == "/agent_lab":
            path = _default_internal_redis_data_path()
    else:
        path = _default_internal_redis_data_path()
    if path.name in {"", ".", ".."} or str(raw).endswith(("/", "\\")):
        path = path / "dump.rdb"
    return path.resolve()


def _internal_redis_paths(config: Dict[str, Any]) -> Dict[str, Path]:
    data_path = _resolve_internal_redis_data_path(config.get("data_path"))
    data_dir = data_path.parent
    socket_raw = str(os.getenv("TATER_REDIS_SOCKET_PATH", "") or "").strip()
    if socket_raw:
        socket_path = Path(socket_raw).expanduser()
        if not socket_path.is_absolute():
            socket_path = (data_dir / socket_path).resolve()
    else:
        socket_path = (data_dir / "redis.sock").resolve()
    return {
        "data_path": data_path,
        "data_dir": data_dir.resolve(),
        "config_path": (data_dir / "redis.conf").resolve(),
        "log_path": (data_dir / "redis.log").resolve(),
        "pid_path": (data_dir / "redis.pid").resolve(),
        "port_path": (data_dir / "redis.port").resolve(),
        "socket_path": socket_path.resolve(),
    }


def _to_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _to_int(
    value: Any,
    *,
    default: int,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    if min_value is not None and out < min_value:
        out = min_value
    if max_value is not None and out > max_value:
        out = max_value
    return out


def _empty_config() -> Dict[str, Any]:
    mode = _normalize_redis_mode(os.getenv("TATER_REDIS_MODE"), default=_REDIS_MODE_INTERNAL)
    return {
        "mode": mode,
        "host": "",
        "port": 6379,
        "db": 0,
        "username": "",
        "password": "",
        "use_tls": False,
        "verify_tls": True,
        "ca_cert_path": "",
        "data_path": str(_resolve_internal_redis_data_path()),
    }


def _normalize_config(raw: Dict[str, Any], *, allow_empty_host: bool = False) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("Redis config payload must be an object.")

    host = str(raw.get("host") or "").strip()
    env_mode = str(os.getenv("TATER_REDIS_MODE", "") or "").strip()
    if env_mode:
        mode = _normalize_redis_mode(env_mode, default=_REDIS_MODE_INTERNAL)
    elif raw.get("mode") is not None:
        mode = _normalize_redis_mode(raw.get("mode"), default=_REDIS_MODE_INTERNAL)
    else:
        # Legacy config files did not have a mode. If they have a host, keep
        # them external; otherwise new installs default to internal Redis.
        mode = _REDIS_MODE_EXTERNAL if host else _REDIS_MODE_INTERNAL

    host = str(raw.get("host") or "").strip()
    if mode == _REDIS_MODE_EXTERNAL and not allow_empty_host and not host:
        raise ValueError("Redis host is required.")

    use_tls = _to_bool(raw.get("use_tls"), default=False)
    verify_tls = _to_bool(raw.get("verify_tls"), default=True)
    ca_cert_path = str(raw.get("ca_cert_path") or "").strip()
    if ca_cert_path:
        ca_cert_path = str(Path(ca_cert_path).expanduser())

    return {
        "mode": mode,
        "host": host,
        "port": _to_int(raw.get("port"), default=6379, min_value=1, max_value=65535),
        "db": _to_int(raw.get("db"), default=0, min_value=0),
        "username": str(raw.get("username") or "").strip(),
        "password": str(raw.get("password") or ""),
        "use_tls": bool(use_tls),
        "verify_tls": bool(verify_tls),
        "ca_cert_path": ca_cert_path,
        "data_path": str(_resolve_internal_redis_data_path(raw.get("data_path"))),
    }


def _public_config(config: Dict[str, Any]) -> Dict[str, Any]:
    mode = _normalize_redis_mode(config.get("mode"), default=_REDIS_MODE_INTERNAL)
    return {
        "mode": mode,
        "internal": mode == _REDIS_MODE_INTERNAL,
        "host": str(config.get("host") or ""),
        "port": int(config.get("port") or 6379),
        "db": int(config.get("db") or 0),
        "username": str(config.get("username") or ""),
        "use_tls": bool(config.get("use_tls")),
        "verify_tls": bool(config.get("verify_tls")),
        "ca_cert_path": str(config.get("ca_cert_path") or ""),
        "password_set": bool(mode == _REDIS_MODE_EXTERNAL and str(config.get("password") or "")),
        "data_path": str(_resolve_internal_redis_data_path(config.get("data_path"))),
    }


def _internal_config_from(config: Dict[str, Any]) -> Dict[str, Any]:
    return _normalize_config(
        {
            "mode": _REDIS_MODE_INTERNAL,
            "host": "",
            "port": 6379,
            "db": int((config or {}).get("db") or 0),
            "username": "",
            "password": "",
            "use_tls": False,
            "verify_tls": True,
            "ca_cert_path": "",
            "data_path": str(_resolve_internal_redis_data_path((config or {}).get("data_path"))),
        },
        allow_empty_host=True,
    )


def _write_config_file(config: Dict[str, Any]) -> None:
    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")
    try:
        os.chmod(tmp_path, 0o600)
    except Exception:
        pass
    tmp_path.replace(path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def _switch_to_internal_config_locked(reason: Any = "") -> Dict[str, Any]:
    global _CONFIG_CACHE, _CONFIG_SOURCE, _CONFIG_ERROR, _CONFIG_FALLBACK_REASON
    previous = _CONFIG_CACHE if isinstance(_CONFIG_CACHE, dict) else _load_config_locked()
    config = _internal_config_from(previous)
    _write_config_file(config)
    _CONFIG_CACHE = dict(config)
    _CONFIG_SOURCE = "internal_fallback"
    _CONFIG_ERROR = ""
    _CONFIG_FALLBACK_REASON = str(reason or "").strip()
    _reset_clients_locked()
    _stop_internal_redis_locked()
    return dict(config)


def _load_config_locked() -> Dict[str, Any]:
    global _CONFIG_CACHE, _CONFIG_SOURCE, _CONFIG_ERROR
    if _CONFIG_CACHE is not None:
        return dict(_CONFIG_CACHE)

    path = _config_path()
    if not path.exists():
        _CONFIG_CACHE = _empty_config()
        _CONFIG_SOURCE = "missing"
        _CONFIG_ERROR = ""
        return dict(_CONFIG_CACHE)

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        _CONFIG_CACHE = _normalize_config(raw, allow_empty_host=True)
        _CONFIG_SOURCE = "file"
        _CONFIG_ERROR = ""
    except Exception as exc:
        _CONFIG_CACHE = _empty_config()
        _CONFIG_SOURCE = "invalid"
        _CONFIG_ERROR = str(exc)
    return dict(_CONFIG_CACHE)


def _close_client(client: Optional[redis.Redis]) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


def _reset_clients_locked() -> None:
    global _TEXT_CLIENT, _BLOB_CLIENT
    _close_client(_TEXT_CLIENT)
    _close_client(_BLOB_CLIENT)
    _TEXT_CLIENT = None
    _BLOB_CLIENT = None


def _quote_redis_conf_value(value: Any) -> str:
    return json.dumps(str(value))


def _tail_text(path: Path, *, max_bytes: int = 4000) -> str:
    try:
        if not path.exists():
            return ""
        data = path.read_bytes()
        if len(data) > max_bytes:
            data = data[-max_bytes:]
        return data.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _find_internal_redis_server_executable() -> Tuple[str, str]:
    raw = str(os.getenv("TATER_REDIS_SERVER_BIN", "") or os.getenv("TATER_INTERNAL_REDIS_SERVER_BIN", "") or "").strip()
    if raw:
        return str(Path(raw).expanduser()), "env"

    try:
        import redislite  # type: ignore

        path = str(getattr(redislite, "__redis_executable__", "") or "").strip()
        if path:
            return path, "redislite"
    except Exception:
        pass

    try:
        import redis_server  # type: ignore

        path = str(getattr(redis_server, "REDIS_SERVER_PATH", "") or "").strip()
        if path:
            return path, "redis_server"
    except Exception:
        pass

    path = shutil.which("redis-server") or ""
    if path:
        return path, "path"
    return "", ""


def _internal_redis_use_unix_socket() -> bool:
    token = str(os.getenv("TATER_REDIS_USE_UNIX_SOCKET", "false") or "").strip().lower()
    return token in {"1", "true", "yes", "on", "enabled"}


def _read_internal_redis_port(path: Path) -> int:
    try:
        return _to_int(path.read_text(encoding="utf-8").strip(), default=0, min_value=0, max_value=65535)
    except Exception:
        return 0


def _write_internal_redis_port(path: Path, port: int) -> None:
    try:
        path.write_text(f"{int(port)}\n", encoding="utf-8")
        _safe_chmod(path, 0o600)
    except Exception:
        pass


def _allocate_loopback_port() -> int:
    raw = str(os.getenv("TATER_REDIS_PORT", "") or os.getenv("TATER_INTERNAL_REDIS_PORT", "") or "").strip()
    if raw:
        return _to_int(raw, default=6379, min_value=1, max_value=65535)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _internal_redis_existing_ping(
    *,
    socket_path: Optional[Path] = None,
    host: str = "127.0.0.1",
    port: int = 0,
    db: int = 0,
    timeout_seconds: float = 0.25,
) -> Tuple[bool, str]:
    kwargs: Dict[str, Any] = {
        "db": int(db),
        "decode_responses": True,
        "socket_timeout": float(timeout_seconds),
        "socket_connect_timeout": float(timeout_seconds),
        "health_check_interval": 30,
    }
    if socket_path is not None:
        if not socket_path.exists():
            return False, "socket not found"
        kwargs["unix_socket_path"] = str(socket_path)
    else:
        if int(port or 0) <= 0:
            return False, "port not set"
        kwargs["host"] = str(host or "127.0.0.1")
        kwargs["port"] = int(port)
    client = None
    try:
        client = redis.Redis(**kwargs)
        client.ping()
        return True, ""
    except Exception as exc:
        return False, str(exc)
    finally:
        _close_client(client)


def _internal_redis_state_key(config: Dict[str, Any]) -> str:
    paths = _internal_redis_paths(config)
    return json.dumps(
        {
            "data_path": str(paths["data_path"]),
            "socket_path": str(paths["socket_path"]),
            "use_unix_socket": _internal_redis_use_unix_socket(),
        },
        sort_keys=True,
    )


def _internal_redis_config_text(paths: Dict[str, Path], *, port: int, use_unix_socket: bool) -> str:
    data_path = paths["data_path"]
    lines = [
        "# Generated by Tater. Do not edit while Tater is running.",
        "daemonize no",
        "supervised no",
        "protected-mode yes",
        "bind 127.0.0.1",
        f"port {0 if use_unix_socket else int(port)}",
    ]
    if use_unix_socket:
        lines.extend(
            [
                f"unixsocket {_quote_redis_conf_value(paths['socket_path'])}",
                "unixsocketperm 700",
            ]
        )
    lines.extend(
        [
            f"dir {_quote_redis_conf_value(paths['data_dir'])}",
            f"dbfilename {_quote_redis_conf_value(data_path.name)}",
            f"pidfile {_quote_redis_conf_value(paths['pid_path'])}",
            f"logfile {_quote_redis_conf_value(paths['log_path'])}",
            "databases 16",
            "appendonly yes",
            "appendfsync everysec",
            "save 900 1",
            "save 300 10",
            "save 60 10000",
            "",
        ]
    )
    return "\n".join(lines)


def _register_internal_redis_shutdown_locked() -> None:
    global _INTERNAL_REDIS_ATEXIT_REGISTERED
    if _INTERNAL_REDIS_ATEXIT_REGISTERED:
        return
    atexit.register(shutdown_internal_redis)
    _INTERNAL_REDIS_ATEXIT_REGISTERED = True


def _stop_internal_redis_locked() -> None:
    global _INTERNAL_REDIS_PROCESS, _INTERNAL_REDIS_INFO
    proc = _INTERNAL_REDIS_PROCESS
    info = dict(_INTERNAL_REDIS_INFO or {})
    _INTERNAL_REDIS_PROCESS = None
    _INTERNAL_REDIS_INFO = {}
    if proc is None or proc.poll() is not None:
        return

    use_unix_socket = bool(info.get("use_unix_socket"))
    client = None
    try:
        if use_unix_socket:
            socket_path = str(info.get("socket_path") or "")
            if not socket_path:
                client = None
            else:
                client = redis.Redis(
                    unix_socket_path=socket_path,
                    decode_responses=True,
                    socket_timeout=1.0,
                    socket_connect_timeout=1.0,
                )
        else:
            port = int(info.get("port") or 0)
            if port > 0:
                client = redis.Redis(
                    host=str(info.get("host") or "127.0.0.1"),
                    port=port,
                    decode_responses=True,
                    socket_timeout=1.0,
                    socket_connect_timeout=1.0,
                )
        if client is not None:
            try:
                client.execute_command("SHUTDOWN", "SAVE")
            except redis.exceptions.ConnectionError:
                pass
    except Exception:
        pass
    finally:
        _close_client(client)

    try:
        proc.wait(timeout=5.0)
    except Exception:
        try:
            proc.terminate()
            proc.wait(timeout=3.0)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def shutdown_internal_redis() -> None:
    with _LOCK:
        _reset_clients_locked()
        _stop_internal_redis_locked()


def _ensure_internal_redis_server_locked(config: Dict[str, Any]) -> Dict[str, Any]:
    global _INTERNAL_REDIS_PROCESS, _INTERNAL_REDIS_INFO
    paths = _internal_redis_paths(config)
    state_key = _internal_redis_state_key(config)
    db = int(config.get("db") or 0)
    use_unix_socket = _internal_redis_use_unix_socket()
    host = "127.0.0.1"

    if _INTERNAL_REDIS_INFO.get("state_key") == state_key:
        proc = _INTERNAL_REDIS_PROCESS
        process_running = proc is None or proc.poll() is None
        if bool(_INTERNAL_REDIS_INFO.get("use_unix_socket")):
            ping_ok, _ping_error = _internal_redis_existing_ping(socket_path=paths["socket_path"], db=db)
        else:
            ping_ok, _ping_error = _internal_redis_existing_ping(
                host=str(_INTERNAL_REDIS_INFO.get("host") or host),
                port=int(_INTERNAL_REDIS_INFO.get("port") or 0),
                db=db,
            )
        if process_running and ping_ok:
            return dict(_INTERNAL_REDIS_INFO)

    if _INTERNAL_REDIS_PROCESS is not None:
        _stop_internal_redis_locked()

    paths["data_dir"].mkdir(parents=True, exist_ok=True)
    existing_port = _read_internal_redis_port(paths["port_path"])
    if use_unix_socket:
        ping_ok, _ping_error = _internal_redis_existing_ping(socket_path=paths["socket_path"], db=db)
    else:
        ping_ok, _ping_error = _internal_redis_existing_ping(host=host, port=existing_port, db=db)
    if ping_ok:
        info = {
            "mode": _REDIS_MODE_INTERNAL,
            "managed": False,
            "pid": 0,
            "state_key": state_key,
            "data_path": str(paths["data_path"]),
            "data_dir": str(paths["data_dir"]),
            "host": host,
            "port": int(existing_port),
            "use_unix_socket": bool(use_unix_socket),
            "socket_path": str(paths["socket_path"]),
            "config_path": str(paths["config_path"]),
            "log_path": str(paths["log_path"]),
            "server_path": "",
            "server_source": "existing",
        }
        _INTERNAL_REDIS_INFO = dict(info)
        return dict(info)

    for cleanup_path in (paths["socket_path"], paths["pid_path"], paths["port_path"]):
        try:
            if cleanup_path.exists() or cleanup_path.is_symlink():
                cleanup_path.unlink()
        except Exception:
            pass

    server_path, server_source = _find_internal_redis_server_executable()
    if not server_path:
        raise RedisNotConfiguredError(
            "Internal Redis requires the `redislite` package, the `redis-server` Python package, "
            "or a redis-server binary on PATH."
        )

    port = 0 if use_unix_socket else _allocate_loopback_port()
    paths["config_path"].write_text(
        _internal_redis_config_text(paths, port=port, use_unix_socket=use_unix_socket),
        encoding="utf-8",
    )
    _safe_chmod(paths["config_path"], 0o600)
    _register_internal_redis_shutdown_locked()

    try:
        proc = subprocess.Popen(
            [server_path, str(paths["config_path"])],
            cwd=str(paths["data_dir"]),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            close_fds=True,
        )
    except Exception as exc:
        raise RedisNotConfiguredError(f"Failed to start internal Redis server: {exc}") from exc

    try:
        timeout = float(os.getenv("TATER_REDIS_START_TIMEOUT_SECONDS", "8") or "8")
    except Exception:
        timeout = 8.0
    timeout = max(1.0, timeout)
    deadline = time.time() + timeout
    last_error = ""
    while time.time() < deadline:
        if proc.poll() is not None:
            detail = _tail_text(paths["log_path"])
            message = detail or f"redis-server exited with code {proc.returncode}"
            raise RedisNotConfiguredError(f"Internal Redis exited during startup: {message}")
        if use_unix_socket:
            ping_ok, ping_error = _internal_redis_existing_ping(
                socket_path=paths["socket_path"],
                db=db,
                timeout_seconds=0.35,
            )
        else:
            ping_ok, ping_error = _internal_redis_existing_ping(
                host=host,
                port=port,
                db=db,
                timeout_seconds=0.35,
            )
        if ping_ok:
            if not use_unix_socket:
                _write_internal_redis_port(paths["port_path"], port)
            info = {
                "mode": _REDIS_MODE_INTERNAL,
                "managed": True,
                "pid": int(proc.pid or 0),
                "state_key": state_key,
                "data_path": str(paths["data_path"]),
                "data_dir": str(paths["data_dir"]),
                "host": host,
                "port": int(port),
                "use_unix_socket": bool(use_unix_socket),
                "socket_path": str(paths["socket_path"]),
                "config_path": str(paths["config_path"]),
                "log_path": str(paths["log_path"]),
                "server_path": str(server_path),
                "server_source": str(server_source),
            }
            _INTERNAL_REDIS_PROCESS = proc
            _INTERNAL_REDIS_INFO = dict(info)
            return dict(info)
        last_error = ping_error
        time.sleep(0.08)

    try:
        proc.terminate()
    except Exception:
        pass
    detail = _tail_text(paths["log_path"]) or last_error or "startup timed out"
    raise RedisNotConfiguredError(f"Internal Redis did not become ready: {detail}")


def _internal_client_kwargs(config: Dict[str, Any], *, decode_responses: bool) -> Dict[str, Any]:
    info = _ensure_internal_redis_server_locked(config)
    kwargs: Dict[str, Any] = {
        "db": int(config.get("db") or 0),
        "decode_responses": bool(decode_responses),
        "socket_timeout": 5.0,
        "socket_connect_timeout": 5.0,
        "health_check_interval": 30,
    }
    if bool(info.get("use_unix_socket")):
        kwargs["unix_socket_path"] = str(info.get("socket_path") or "")
    else:
        kwargs["host"] = str(info.get("host") or "127.0.0.1")
        kwargs["port"] = int(info.get("port") or 0)
    return kwargs


def _client_kwargs(config: Dict[str, Any], *, decode_responses: bool) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "host": str(config.get("host") or ""),
        "port": int(config.get("port") or 6379),
        "db": int(config.get("db") or 0),
        "decode_responses": bool(decode_responses),
        "socket_timeout": 5.0,
        "socket_connect_timeout": 5.0,
        "health_check_interval": 30,
    }
    username = str(config.get("username") or "").strip()
    password = str(config.get("password") or "")
    if username:
        kwargs["username"] = username
    if password:
        kwargs["password"] = password

    if bool(config.get("use_tls")):
        kwargs["ssl"] = True
        if bool(config.get("verify_tls")):
            kwargs["ssl_cert_reqs"] = ssl.CERT_REQUIRED
        else:
            kwargs["ssl_cert_reqs"] = ssl.CERT_NONE
        ca_cert_path = str(config.get("ca_cert_path") or "").strip()
        if ca_cert_path:
            kwargs["ssl_ca_certs"] = ca_cert_path
    return kwargs


def get_redis_connection_config(*, include_secret: bool = False) -> Dict[str, Any]:
    with _LOCK:
        config = _load_config_locked()
    if include_secret:
        return dict(config)
    return _public_config(config)


def reload_redis_connection_config() -> Dict[str, Any]:
    global _CONFIG_CACHE
    with _LOCK:
        _CONFIG_CACHE = None
        _reset_clients_locked()
        _stop_internal_redis_locked()
        config = _load_config_locked()
    return _public_config(config)


def save_redis_connection_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = _normalize_config(payload, allow_empty_host=False)
    _write_config_file(config)

    global _CONFIG_CACHE, _CONFIG_SOURCE, _CONFIG_ERROR, _CONFIG_FALLBACK_REASON
    with _LOCK:
        _CONFIG_CACHE = dict(config)
        _CONFIG_SOURCE = "file"
        _CONFIG_ERROR = ""
        _CONFIG_FALLBACK_REASON = ""
        _reset_clients_locked()
        _stop_internal_redis_locked()
    return _public_config(config)


def _redis_type_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip().lower()
    return str(value or "").strip().lower()


def _copy_redis_key_by_type(source: redis.Redis, target: redis.Redis, key: bytes, key_type: str) -> None:
    target.delete(key)
    if key_type == "string":
        value = source.get(key)
        if value is not None:
            target.set(key, value)
        return

    if key_type == "hash":
        mapping = source.hgetall(key) or {}
        if mapping:
            target.hset(key, mapping=mapping)
        return

    if key_type == "list":
        values = source.lrange(key, 0, -1) or []
        if values:
            target.rpush(key, *values)
        return

    if key_type == "set":
        values = source.smembers(key) or set()
        if values:
            target.sadd(key, *values)
        return

    if key_type == "zset":
        rows = source.zrange(key, 0, -1, withscores=True) or []
        if rows:
            target.zadd(key, {member: float(score) for member, score in rows})
        return

    if key_type == "stream":
        last_id = "-"
        while True:
            raw_rows = source.xrange(key, min=last_id, max="+", count=500) or []
            rows = raw_rows
            if last_id != "-":
                rows = rows[1:]
            if not rows:
                break
            for entry_id, fields in rows:
                target.xadd(key, fields or {}, id=entry_id)
                last_id = entry_id
            if len(raw_rows) < 500:
                break
        return

    raise RedisError(f"Unsupported Redis key type for migration: {key_type or 'unknown'}")


def _copy_redis_key(source: redis.Redis, target: redis.Redis, key: bytes) -> str:
    key_type = _redis_type_text(source.type(key))
    if not key_type or key_type == "none":
        return "missing"

    ttl_ms = int(source.pttl(key))
    if ttl_ms == -2:
        return "missing"
    restore_ttl = max(0, ttl_ms)

    try:
        payload = source.dump(key)
        if payload is None:
            return "missing"
        target.restore(key, restore_ttl, payload, replace=True)
        return "dump_restore"
    except Exception:
        _copy_redis_key_by_type(source, target, key, key_type)
        if ttl_ms >= 0:
            target.pexpire(key, ttl_ms)
        return key_type


def migrate_current_redis_to_internal(
    *,
    data_path: Any = "",
    flush_internal: bool = True,
) -> Dict[str, Any]:
    with _LOCK:
        source_config = _load_config_locked()
        source_mode = _normalize_redis_mode(source_config.get("mode"), default=_REDIS_MODE_INTERNAL)
        if source_mode == _REDIS_MODE_INTERNAL:
            return {
                "already_internal": True,
                "switched": False,
                "keys_scanned": 0,
                "keys_restored": 0,
                "keys_failed": 0,
                "target": _public_config(source_config),
            }
        if not str(source_config.get("host") or "").strip():
            raise RedisNotConfiguredError("External Redis is not configured.")

        target_config = dict(source_config)
        target_config.update(
            {
                "mode": _REDIS_MODE_INTERNAL,
                "host": "",
                "port": 6379,
                "username": "",
                "password": "",
                "use_tls": False,
                "verify_tls": True,
                "ca_cert_path": "",
                "data_path": str(_resolve_internal_redis_data_path(data_path or source_config.get("data_path"))),
            }
        )
        source_kwargs = _client_kwargs(source_config, decode_responses=False)
        target_kwargs = _internal_client_kwargs(target_config, decode_responses=False)

    source = redis.Redis(**source_kwargs)
    target = redis.Redis(**target_kwargs)
    keys_scanned = 0
    keys_restored = 0
    keys_failed = 0
    failed: List[Dict[str, str]] = []
    copied_by_method: Dict[str, int] = {}
    migration_ok = False

    try:
        source.ping()
        target.ping()
        same_server = False
        try:
            source_run_id = str((source.info("server") or {}).get("run_id") or "")
            target_run_id = str((target.info("server") or {}).get("run_id") or "")
            same_server = bool(source_run_id and source_run_id == target_run_id)
        except Exception:
            same_server = False

        if same_server:
            try:
                keys_scanned = int(source.dbsize() or 0)
            except Exception:
                keys_scanned = 0
            keys_restored = keys_scanned
            copied_by_method["same_server"] = keys_restored
        else:
            if bool(flush_internal):
                target.flushdb()

            for raw_key in source.scan_iter(match=b"*", count=500):
                if raw_key is None:
                    continue
                key = bytes(raw_key)
                keys_scanned += 1
                try:
                    method = _copy_redis_key(source, target, key)
                    if method == "missing":
                        continue
                    keys_restored += 1
                    copied_by_method[method] = int(copied_by_method.get(method) or 0) + 1
                except Exception as exc:
                    keys_failed += 1
                    if len(failed) < 25:
                        failed.append({"key": key.decode("utf-8", errors="replace"), "error": str(exc)})

            if keys_failed:
                raise RedisError(f"Failed to migrate {keys_failed} Redis key(s). First failures: {failed}")

        try:
            target.save()
        except Exception:
            pass
        migration_ok = True
    finally:
        _close_client(source)
        _close_client(target)
        if not migration_ok:
            with _LOCK:
                _stop_internal_redis_locked()

    saved_public = save_redis_connection_settings(target_config)
    status = get_redis_connection_status()
    return {
        "already_internal": False,
        "switched": True,
        "keys_scanned": int(keys_scanned),
        "keys_restored": int(keys_restored),
        "keys_failed": int(keys_failed),
        "failed": failed,
        "copied_by_method": copied_by_method,
        "target": saved_public,
        "redis_status": status,
    }


def test_redis_connection_settings(
    payload: Dict[str, Any],
    *,
    timeout_seconds: float = 3.0,
) -> Tuple[bool, str]:
    try:
        config = _normalize_config(payload, allow_empty_host=False)
    except Exception as exc:
        return False, str(exc)

    client = None
    try:
        if _normalize_redis_mode(config.get("mode"), default=_REDIS_MODE_INTERNAL) == _REDIS_MODE_INTERNAL:
            with _LOCK:
                kwargs = _internal_client_kwargs(config, decode_responses=True)
        else:
            kwargs = _client_kwargs(config, decode_responses=True)
        kwargs["socket_timeout"] = float(timeout_seconds)
        kwargs["socket_connect_timeout"] = float(timeout_seconds)
        client = redis.Redis(**kwargs)
        client.ping()
        return True, ""
    except Exception as exc:
        return False, str(exc)
    finally:
        _close_client(client)


def get_redis_connection_status() -> Dict[str, Any]:
    with _LOCK:
        config = _load_config_locked()
        source = str(_CONFIG_SOURCE or "unknown")
        load_error = str(_CONFIG_ERROR or "")
        fallback_reason = str(_CONFIG_FALLBACK_REASON or "")
    mode = _normalize_redis_mode(config.get("mode"), default=_REDIS_MODE_INTERNAL)
    configured = mode == _REDIS_MODE_INTERNAL or bool(str(config.get("host") or "").strip())
    connected = False
    error = ""
    if configured:
        try:
            get_redis_client(decode_responses=True).ping()
            with _LOCK:
                config = _load_config_locked()
                source = str(_CONFIG_SOURCE or source)
                fallback_reason = str(_CONFIG_FALLBACK_REASON or fallback_reason)
            mode = _normalize_redis_mode(config.get("mode"), default=_REDIS_MODE_INTERNAL)
            configured = mode == _REDIS_MODE_INTERNAL or bool(str(config.get("host") or "").strip())
            connected = True
        except Exception as exc:
            if mode == _REDIS_MODE_EXTERNAL:
                fallback_error = str(exc)
                try:
                    with _LOCK:
                        config = _switch_to_internal_config_locked(fallback_error)
                        source = str(_CONFIG_SOURCE or "internal_fallback")
                        fallback_reason = str(_CONFIG_FALLBACK_REASON or "")
                    get_redis_client(decode_responses=True).ping()
                    mode = _REDIS_MODE_INTERNAL
                    configured = True
                    connected = True
                    error = ""
                except Exception as fallback_exc:
                    connected = False
                    error = str(fallback_exc)
            else:
                connected = False
                error = str(exc)
    else:
        try:
            with _LOCK:
                config = _switch_to_internal_config_locked(load_error or "Redis connection is not configured.")
                source = str(_CONFIG_SOURCE or "internal_fallback")
                fallback_reason = str(_CONFIG_FALLBACK_REASON or "")
            get_redis_client(decode_responses=True).ping()
            mode = _REDIS_MODE_INTERNAL
            configured = True
            connected = True
            error = ""
        except Exception as fallback_exc:
            error = str(fallback_exc)

    public = _public_config(config)
    status = {
        "configured": configured,
        "connected": connected,
        "source": source,
        "error": error,
        "fallback_reason": fallback_reason,
        "config_path": str(_config_path()),
        **public,
    }
    if mode == _REDIS_MODE_INTERNAL:
        paths = _internal_redis_paths(config)
        info = dict(_INTERNAL_REDIS_INFO or {})
        status.update(
            {
                "data_path": str(paths["data_path"]),
                "data_dir": str(paths["data_dir"]),
                "internal_host": str(info.get("host") or "127.0.0.1"),
                "internal_port": int(info.get("port") or _read_internal_redis_port(paths["port_path"])),
                "internal_use_unix_socket": bool(info.get("use_unix_socket") or _internal_redis_use_unix_socket()),
                "socket_path": str(info.get("socket_path") or paths["socket_path"]),
                "redis_config_path": str(info.get("config_path") or paths["config_path"]),
                "redis_log_path": str(info.get("log_path") or paths["log_path"]),
                "redis_pid": int(info.get("pid") or 0),
                "redis_managed": bool(info.get("managed")),
                "redis_server_path": str(info.get("server_path") or ""),
                "redis_server_source": str(info.get("server_source") or ""),
            }
        )
    return status


def get_redis_client(*, decode_responses: bool = True) -> redis.Redis:
    global _TEXT_CLIENT, _BLOB_CLIENT
    with _LOCK:
        config = _load_config_locked()
        mode = _normalize_redis_mode(config.get("mode"), default=_REDIS_MODE_INTERNAL)
        host = str(config.get("host") or "").strip()
        if mode == _REDIS_MODE_EXTERNAL and not host:
            config = _switch_to_internal_config_locked("External Redis host is empty.")
            mode = _REDIS_MODE_INTERNAL
        if decode_responses:
            if _TEXT_CLIENT is None:
                kwargs = (
                    _internal_client_kwargs(config, decode_responses=True)
                    if mode == _REDIS_MODE_INTERNAL
                    else _client_kwargs(config, decode_responses=True)
                )
                candidate = redis.Redis(**kwargs)
                if mode == _REDIS_MODE_EXTERNAL:
                    try:
                        candidate.ping()
                    except Exception as exc:
                        _close_client(candidate)
                        config = _switch_to_internal_config_locked(exc)
                        mode = _REDIS_MODE_INTERNAL
                        candidate = redis.Redis(**_internal_client_kwargs(config, decode_responses=True))
                _TEXT_CLIENT = candidate
            return _TEXT_CLIENT
        if _BLOB_CLIENT is None:
            kwargs = (
                _internal_client_kwargs(config, decode_responses=False)
                if mode == _REDIS_MODE_INTERNAL
                else _client_kwargs(config, decode_responses=False)
            )
            candidate = redis.Redis(**kwargs)
            if mode == _REDIS_MODE_EXTERNAL:
                try:
                    candidate.ping()
                except Exception as exc:
                    _close_client(candidate)
                    config = _switch_to_internal_config_locked(exc)
                    mode = _REDIS_MODE_INTERNAL
                    candidate = redis.Redis(**_internal_client_kwargs(config, decode_responses=False))
            _BLOB_CLIENT = candidate
        return _BLOB_CLIENT


class EncryptedRedisPipelineProxy:
    def __init__(self, pipeline: Any, *, decode_responses: bool):
        self._pipeline = pipeline
        self._decode_responses = bool(decode_responses)

    def _should_encrypt(self, key: Any) -> bool:
        return _live_encryption_enabled() and not _key_requires_plaintext_counter(key)

    def _encode(self, value: Any, key: Any) -> Any:
        if not self._should_encrypt(key):
            return value
        return _encrypt_value(value, decode_responses=self._decode_responses)

    def set(self, name: Any, value: Any, *args, **kwargs):
        self._pipeline.set(name, self._encode(value, name), *args, **kwargs)
        return self

    def hset(self, name: Any, key: Any = None, value: Any = None, mapping: Optional[Dict[Any, Any]] = None, items: Any = None):
        if mapping is not None:
            encoded = {field: self._encode(raw_value, name) for field, raw_value in dict(mapping).items()}
            self._pipeline.hset(name, mapping=encoded)
            return self
        if items is not None:
            encoded = {field: self._encode(raw_value, name) for field, raw_value in dict(items).items()}
            self._pipeline.hset(name, mapping=encoded)
            return self
        self._pipeline.hset(name, key, self._encode(value, name))
        return self

    def rpush(self, name: Any, *values: Any):
        encoded = [self._encode(value, name) for value in values]
        self._pipeline.rpush(name, *encoded)
        return self

    def lpush(self, name: Any, *values: Any):
        encoded = [self._encode(value, name) for value in values]
        self._pipeline.lpush(name, *encoded)
        return self

    def zadd(self, name: Any, mapping: Dict[Any, Any], *args, **kwargs):
        # Keep ZSET members plaintext (identity semantics for zadd/zrem/zscore/zincrby).
        self._pipeline.zadd(name, dict(mapping or {}), *args, **kwargs)
        return self

    def sadd(self, name: Any, *values: Any):
        # Keep SET members plaintext (identity semantics for sadd/srem/sismember).
        self._pipeline.sadd(name, *values)
        return self

    def execute(self, *args, **kwargs):
        return self._pipeline.execute(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pipeline, name)


class EncryptedRedisClientFacade:
    def __init__(self, client: redis.Redis, *, decode_responses: bool):
        self._client = client
        self._decode_responses = bool(decode_responses)

    def _should_encrypt(self, key: Any) -> bool:
        return _live_encryption_enabled() and not _key_requires_plaintext_counter(key)

    def _encode(self, value: Any, key: Any) -> Any:
        if not self._should_encrypt(key):
            return value
        return _encrypt_value(value, decode_responses=self._decode_responses)

    def _decode(self, value: Any) -> Any:
        return _decrypt_value(value, decode_responses=self._decode_responses)

    def _decoded_identity_bytes(self, value: Any) -> bytes:
        return _value_bytes(_decrypt_value(value, decode_responses=self._decode_responses))

    def _find_legacy_zset_members(self, name: Any, members: List[Any]) -> List[Any]:
        if not members:
            return []
        rows = self._client.zrange(name, 0, -1) or []
        if not isinstance(rows, list):
            return []
        target_bytes = [_value_bytes(member) for member in members]
        out: List[Any] = []
        for raw_member in rows:
            decoded_bytes = self._decoded_identity_bytes(raw_member)
            for token in target_bytes:
                if decoded_bytes == token and _value_bytes(raw_member) != token:
                    out.append(raw_member)
                    break
        return out

    def _find_legacy_set_members(self, name: Any, members: List[Any]) -> List[Any]:
        if not members:
            return []
        rows = self._client.smembers(name) or set()
        if not isinstance(rows, set):
            return []
        target_bytes = [_value_bytes(member) for member in members]
        out: List[Any] = []
        for raw_member in rows:
            decoded_bytes = self._decoded_identity_bytes(raw_member)
            for token in target_bytes:
                if decoded_bytes == token and _value_bytes(raw_member) != token:
                    out.append(raw_member)
                    break
        return out

    def set(self, name: Any, value: Any, *args, **kwargs):
        return self._client.set(name, self._encode(value, name), *args, **kwargs)

    def get(self, name: Any):
        return self._decode(self._client.get(name))

    def mget(self, keys: Any, *args, **kwargs):
        rows = self._client.mget(keys, *args, **kwargs)
        if not isinstance(rows, list):
            return rows
        return [self._decode(value) for value in rows]

    def hset(self, name: Any, key: Any = None, value: Any = None, mapping: Optional[Dict[Any, Any]] = None, items: Any = None):
        if mapping is not None:
            encoded = {field: self._encode(raw_value, name) for field, raw_value in dict(mapping).items()}
            return self._client.hset(name, mapping=encoded)
        if items is not None:
            encoded = {field: self._encode(raw_value, name) for field, raw_value in dict(items).items()}
            return self._client.hset(name, mapping=encoded)
        return self._client.hset(name, key, self._encode(value, name))

    def hget(self, name: Any, key: Any):
        return self._decode(self._client.hget(name, key))

    def hgetall(self, name: Any):
        raw = self._client.hgetall(name)
        if not isinstance(raw, dict):
            return raw
        return {field: self._decode(value) for field, value in raw.items()}

    def rpush(self, name: Any, *values: Any):
        encoded = [self._encode(value, name) for value in values]
        return self._client.rpush(name, *encoded)

    def lpush(self, name: Any, *values: Any):
        encoded = [self._encode(value, name) for value in values]
        return self._client.lpush(name, *encoded)

    def lrange(self, name: Any, start: int, end: int):
        rows = self._client.lrange(name, start, end)
        if not isinstance(rows, list):
            return rows
        return [self._decode(value) for value in rows]

    def lpop(self, name: Any, count: Any = None):
        if count is None:
            return self._decode(self._client.lpop(name))
        rows = self._client.lpop(name, count)
        if not isinstance(rows, list):
            return rows
        return [self._decode(value) for value in rows]

    def rpop(self, name: Any, count: Any = None):
        if count is None:
            return self._decode(self._client.rpop(name))
        rows = self._client.rpop(name, count)
        if not isinstance(rows, list):
            return rows
        return [self._decode(value) for value in rows]

    def blpop(self, keys: Any, timeout: int = 0):
        row = self._client.blpop(keys, timeout=timeout)
        if not (isinstance(row, tuple) and len(row) == 2):
            return row
        key, value = row
        return key, self._decode(value)

    def brpop(self, keys: Any, timeout: int = 0):
        row = self._client.brpop(keys, timeout=timeout)
        if not (isinstance(row, tuple) and len(row) == 2):
            return row
        key, value = row
        return key, self._decode(value)

    def sadd(self, name: Any, *values: Any):
        return self._client.sadd(name, *values)

    def smembers(self, name: Any):
        rows = self._client.smembers(name)
        if not isinstance(rows, set):
            return rows
        return {self._decode(value) for value in rows}

    def srem(self, name: Any, *values: Any):
        removed = self._client.srem(name, *values)
        if int(removed or 0) > 0 or not self._should_encrypt(name):
            return removed
        legacy = self._find_legacy_set_members(name, list(values))
        if not legacy:
            return removed
        return int(removed or 0) + int(self._client.srem(name, *legacy) or 0)

    def sismember(self, name: Any, value: Any):
        if self._client.sismember(name, value):
            return True
        if not self._should_encrypt(name):
            return False
        rows = self._client.smembers(name) or set()
        if not isinstance(rows, set):
            return False
        target = _value_bytes(value)
        for raw_member in rows:
            if self._decoded_identity_bytes(raw_member) == target:
                return True
        return False

    def zadd(self, name: Any, mapping: Dict[Any, Any], *args, **kwargs):
        normalized = dict(mapping or {})
        if self._should_encrypt(name) and normalized:
            legacy = self._find_legacy_zset_members(name, list(normalized.keys()))
            if legacy:
                self._client.zrem(name, *legacy)
        return self._client.zadd(name, normalized, *args, **kwargs)

    def zrange(self, name: Any, start: int, end: int, *args, **kwargs):
        rows = self._client.zrange(name, start, end, *args, **kwargs)
        if not isinstance(rows, list):
            return rows
        if rows and isinstance(rows[0], tuple):
            out = []
            for member, score in rows:
                out.append((self._decode(member), score))
            return out
        return [self._decode(member) for member in rows]

    def zrem(self, name: Any, *members: Any):
        removed = self._client.zrem(name, *members)
        if int(removed or 0) > 0 or not self._should_encrypt(name):
            return removed
        legacy = self._find_legacy_zset_members(name, list(members))
        if not legacy:
            return removed
        return int(removed or 0) + int(self._client.zrem(name, *legacy) or 0)

    def zscore(self, name: Any, value: Any):
        score = self._client.zscore(name, value)
        if score is not None or not self._should_encrypt(name):
            return score
        rows = self._client.zrange(name, 0, -1, withscores=True) or []
        if not isinstance(rows, list):
            return None
        target = _value_bytes(value)
        for row in rows:
            if not (isinstance(row, tuple) and len(row) == 2):
                continue
            raw_member, raw_score = row
            if self._decoded_identity_bytes(raw_member) == target:
                return raw_score
        return None

    def zincrby(self, name: Any, amount: Any, value: Any):
        if not self._should_encrypt(name):
            return self._client.zincrby(name, amount, value)
        legacy = self._find_legacy_zset_members(name, [value])
        if legacy:
            # If legacy encrypted members exist for this logical value, normalize first.
            self._client.zrem(name, *legacy)
        return self._client.zincrby(name, amount, value)

    def zrevrange(self, name: Any, start: int, end: int, *args, **kwargs):
        rows = self._client.zrevrange(name, start, end, *args, **kwargs)
        if not isinstance(rows, list):
            return rows
        if rows and isinstance(rows[0], tuple):
            out = []
            for member, score in rows:
                out.append((self._decode(member), score))
            return out
        return [self._decode(member) for member in rows]

    def zrevrangebyscore(self, name: Any, max_score: Any, min_score: Any, *args, **kwargs):
        rows = self._client.zrevrangebyscore(name, max_score, min_score, *args, **kwargs)
        if not isinstance(rows, list):
            return rows
        if rows and isinstance(rows[0], tuple):
            out = []
            for member, score in rows:
                out.append((self._decode(member), score))
            return out
        return [self._decode(member) for member in rows]

    def pipeline(self, *args, **kwargs):
        raw_pipeline = self._client.pipeline(*args, **kwargs)
        return EncryptedRedisPipelineProxy(raw_pipeline, decode_responses=self._decode_responses)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class RedisClientProxy:
    def __init__(self, *, decode_responses: bool):
        self._decode_responses = bool(decode_responses)

    def _client(self) -> EncryptedRedisClientFacade:
        raw = get_redis_client(decode_responses=self._decode_responses)
        return EncryptedRedisClientFacade(raw, decode_responses=self._decode_responses)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client(), name)

    def __repr__(self) -> str:
        mode = "text" if self._decode_responses else "binary"
        return f"<RedisClientProxy mode={mode}>"


redis_client = RedisClientProxy(decode_responses=True)
redis_blob_client = RedisClientProxy(decode_responses=False)
