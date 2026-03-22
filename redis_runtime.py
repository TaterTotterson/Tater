import hashlib
import json
import os
import ssl
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

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
_RUNTIME_DIR = (Path(__file__).resolve().parent / ".runtime").resolve()
_REDIS_ENCRYPTION_KEY_PATH = (_RUNTIME_DIR / "redis_encryption.key").resolve()
_REDIS_LIVE_ENCRYPTION_STATE_PATH = (_RUNTIME_DIR / "redis_live_encryption.json").resolve()
_REDIS_ENCRYPTED_SNAPSHOT_PATH = (_RUNTIME_DIR / "redis_snapshot.enc").resolve()  # legacy artifact path

_LIVE_ENCRYPTION_STATE_CACHE: Optional[Dict[str, Any]] = None

_REDIS_ENCRYPTION_PREFIX_TEXT = "enc:v1:"
_REDIS_ENCRYPTION_PREFIX_BYTES = _REDIS_ENCRYPTION_PREFIX_TEXT.encode("ascii")

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
            next_value = _encrypt_value(raw_value, decode_responses=False) if encrypt_mode else _decrypt_value(raw_value, decode_responses=False)
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
            next_member = _encrypt_value(raw_member, decode_responses=False) if encrypt_mode else _decrypt_value(raw_member, decode_responses=False)
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
    return {
        "host": "",
        "port": 6379,
        "db": 0,
        "username": "",
        "password": "",
        "use_tls": False,
        "verify_tls": True,
        "ca_cert_path": "",
    }


def _normalize_config(raw: Dict[str, Any], *, allow_empty_host: bool = False) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("Redis config payload must be an object.")

    host = str(raw.get("host") or "").strip()
    if not allow_empty_host and not host:
        raise ValueError("Redis host is required.")

    use_tls = _to_bool(raw.get("use_tls"), default=False)
    verify_tls = _to_bool(raw.get("verify_tls"), default=True)
    ca_cert_path = str(raw.get("ca_cert_path") or "").strip()
    if ca_cert_path:
        ca_cert_path = str(Path(ca_cert_path).expanduser())

    return {
        "host": host,
        "port": _to_int(raw.get("port"), default=6379, min_value=1, max_value=65535),
        "db": _to_int(raw.get("db"), default=0, min_value=0),
        "username": str(raw.get("username") or "").strip(),
        "password": str(raw.get("password") or ""),
        "use_tls": bool(use_tls),
        "verify_tls": bool(verify_tls),
        "ca_cert_path": ca_cert_path,
    }


def _public_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "host": str(config.get("host") or ""),
        "port": int(config.get("port") or 6379),
        "db": int(config.get("db") or 0),
        "username": str(config.get("username") or ""),
        "use_tls": bool(config.get("use_tls")),
        "verify_tls": bool(config.get("verify_tls")),
        "ca_cert_path": str(config.get("ca_cert_path") or ""),
        "password_set": bool(str(config.get("password") or "")),
    }


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
        config = _load_config_locked()
    return _public_config(config)


def save_redis_connection_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = _normalize_config(payload, allow_empty_host=False)
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

    global _CONFIG_CACHE, _CONFIG_SOURCE, _CONFIG_ERROR
    with _LOCK:
        _CONFIG_CACHE = dict(config)
        _CONFIG_SOURCE = "file"
        _CONFIG_ERROR = ""
        _reset_clients_locked()
    return _public_config(config)


def test_redis_connection_settings(
    payload: Dict[str, Any],
    *,
    timeout_seconds: float = 3.0,
) -> Tuple[bool, str]:
    try:
        config = _normalize_config(payload, allow_empty_host=False)
    except Exception as exc:
        return False, str(exc)

    kwargs = _client_kwargs(config, decode_responses=True)
    kwargs["socket_timeout"] = float(timeout_seconds)
    kwargs["socket_connect_timeout"] = float(timeout_seconds)
    client = None
    try:
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
    configured = bool(str(config.get("host") or "").strip())
    connected = False
    error = ""
    if configured:
        try:
            get_redis_client(decode_responses=True).ping()
            connected = True
        except Exception as exc:
            connected = False
            error = str(exc)
    else:
        error = load_error or "Redis connection is not configured."

    return {
        "configured": configured,
        "connected": connected,
        "source": source,
        "error": error,
        "config_path": str(_config_path()),
        **_public_config(config),
    }


def get_redis_client(*, decode_responses: bool = True) -> redis.Redis:
    global _TEXT_CLIENT, _BLOB_CLIENT
    with _LOCK:
        config = _load_config_locked()
        host = str(config.get("host") or "").strip()
        if not host:
            raise RedisNotConfiguredError("Redis is not configured. Open TaterOS WebUI and complete Redis setup.")
        if decode_responses:
            if _TEXT_CLIENT is None:
                _TEXT_CLIENT = redis.Redis(**_client_kwargs(config, decode_responses=True))
            return _TEXT_CLIENT
        if _BLOB_CLIENT is None:
            _BLOB_CLIENT = redis.Redis(**_client_kwargs(config, decode_responses=False))
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
        encoded = {self._encode(member, name): score for member, score in dict(mapping or {}).items()}
        self._pipeline.zadd(name, encoded, *args, **kwargs)
        return self

    def sadd(self, name: Any, *values: Any):
        encoded = [self._encode(value, name) for value in values]
        self._pipeline.sadd(name, *encoded)
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

    def sadd(self, name: Any, *values: Any):
        encoded = [self._encode(value, name) for value in values]
        return self._client.sadd(name, *encoded)

    def smembers(self, name: Any):
        rows = self._client.smembers(name)
        if not isinstance(rows, set):
            return rows
        return {self._decode(value) for value in rows}

    def srem(self, name: Any, *values: Any):
        encoded = [self._encode(value, name) for value in values]
        return self._client.srem(name, *encoded)

    def zadd(self, name: Any, mapping: Dict[Any, Any], *args, **kwargs):
        encoded = {self._encode(member, name): score for member, score in dict(mapping or {}).items()}
        return self._client.zadd(name, encoded, *args, **kwargs)

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
        encoded = [self._encode(member, name) for member in members]
        return self._client.zrem(name, *encoded)

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
