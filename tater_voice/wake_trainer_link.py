from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.parse import urlparse

from helpers import redis_client


LINK_HASH_KEY = "voice_wake_trainer_link"
PAIRING_HASH_KEY = "voice_wake_trainer_pairing_sessions"
TRAINER_LINK_HEADER = "X-Tater-Trainer-Token"
PAIRING_TTL_S = 10 * 60
PAIRING_CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
_PAIRING_LOCK = threading.RLock()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _token_digest(token: Any) -> str:
    return hashlib.sha256(_text(token).encode("utf-8")).hexdigest()


def _code_digest(value: Any) -> str:
    normalized = "".join(ch for ch in _text(value).upper() if ch.isalnum())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _http_base_url(value: Any, *, label: str) -> str:
    token = _text(value).rstrip("/")
    parsed = urlparse(token)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"{label} must start with http:// or https://.")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f"{label} must be a plain server address.")
    return token


def _raw_status() -> Dict[str, Any]:
    try:
        return dict(redis_client.hgetall(LINK_HASH_KEY) or {})
    except Exception:
        return {}


def status() -> Dict[str, Any]:
    raw = _raw_status()
    linked = bool(_text(raw.get("token_hash")) and _text(raw.get("trainer_id")))
    return {
        "linked": linked,
        "trainer_id": _text(raw.get("trainer_id")),
        "trainer_name": _text(raw.get("trainer_name")) or "Wake Word Trainer",
        "trainer_url": _text(raw.get("trainer_url")),
        "publish_base_url": _text(raw.get("publish_base_url")),
        "linked_at": _text(raw.get("linked_at")),
        "last_publish_at": _text(raw.get("last_publish_at")),
        "last_wake_word": _text(raw.get("last_wake_word")),
        "last_wake_word_url": _text(raw.get("last_wake_word_url")),
    }


def _read_pairing(pairing_id: Any) -> Dict[str, Any]:
    token = _text(pairing_id)
    if not token:
        return {}
    try:
        raw = redis_client.hget(PAIRING_HASH_KEY, token)
        payload = json.loads(raw) if raw else {}
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_pairing(pairing_id: str, payload: Dict[str, Any]) -> None:
    redis_client.hset(PAIRING_HASH_KEY, pairing_id, json.dumps(payload, separators=(",", ":")))


def _delete_pairing(pairing_id: Any) -> None:
    token = _text(pairing_id)
    if token:
        redis_client.hdel(PAIRING_HASH_KEY, token)


def _active_pairings() -> Dict[str, Dict[str, Any]]:
    now = time.time()
    active: Dict[str, Dict[str, Any]] = {}
    try:
        rows = dict(redis_client.hgetall(PAIRING_HASH_KEY) or {})
    except Exception:
        rows = {}
    for pairing_id, raw in rows.items():
        try:
            row = json.loads(raw)
        except Exception:
            row = {}
        if not isinstance(row, dict) or float(row.get("expires_at") or 0.0) <= now:
            _delete_pairing(pairing_id)
            continue
        active[_text(pairing_id)] = row
    return active


def start_pairing() -> Dict[str, Any]:
    pairing_id = secrets.token_urlsafe(18)
    raw_code = "".join(secrets.choice(PAIRING_CODE_ALPHABET) for _ in range(8))
    now = time.time()
    row = {
        "code_hash": _code_digest(raw_code),
        "state": "waiting",
        "created_at": now,
        "expires_at": now + PAIRING_TTL_S,
        "trainer_name": "",
        "linked_at": "",
    }
    with _PAIRING_LOCK:
        _active_pairings()
        _write_pairing(pairing_id, row)
    return {
        "ok": True,
        "pairing_id": pairing_id,
        "display_code": f"{raw_code[:4]}-{raw_code[4:]}",
        "state": "waiting",
        "expires_in_s": PAIRING_TTL_S,
    }


def pairing_status(pairing_id: Any) -> Dict[str, Any]:
    row = _read_pairing(pairing_id)
    if not row or float(row.get("expires_at") or 0.0) <= time.time():
        _delete_pairing(pairing_id)
        return {
            "ok": True,
            "pairing_id": _text(pairing_id),
            "state": "expired",
            "expired": True,
            "expires_in_s": 0,
        }
    state = _text(row.get("state")).lower() or "waiting"
    return {
        "ok": True,
        "pairing_id": _text(pairing_id),
        "state": state,
        "linked": state == "linked",
        "trainer_name": _text(row.get("trainer_name")),
        "linked_at": _text(row.get("linked_at")),
        "expires_in_s": max(0, int(float(row.get("expires_at") or 0.0) - time.time())),
    }


def claim_pairing(
    *,
    pairing_code: Any,
    trainer_id: Any,
    trainer_name: Any,
    trainer_url: Any,
    publish_base_url: Any,
) -> Dict[str, Any]:
    supplied_hash = _code_digest(pairing_code)
    safe_trainer_id = _text(trainer_id)
    if not safe_trainer_id:
        raise ValueError("Trainer identity is required.")
    safe_trainer_url = _http_base_url(trainer_url, label="Trainer URL")
    safe_publish_url = _http_base_url(publish_base_url or safe_trainer_url, label="Trainer public URL")

    with _PAIRING_LOCK:
        pairings = _active_pairings()
        matched_id = ""
        matched_row: Dict[str, Any] = {}
        for pairing_id, row in pairings.items():
            if (
                _text(row.get("state")).lower() == "waiting"
                and hmac.compare_digest(supplied_hash, _text(row.get("code_hash")))
            ):
                matched_id = pairing_id
                matched_row = row
                break
        if not matched_id:
            raise ValueError("Pairing code is invalid or expired.")

        link_token = secrets.token_urlsafe(32)
        linked_at = _iso_now()
        safe_trainer_name = _text(trainer_name) or "Wake Word Trainer"
        redis_client.hset(
            LINK_HASH_KEY,
            mapping={
                "token_hash": _token_digest(link_token),
                "trainer_id": safe_trainer_id,
                "trainer_name": safe_trainer_name,
                "trainer_url": safe_trainer_url,
                "publish_base_url": safe_publish_url,
                "linked_at": linked_at,
                "last_publish_at": "",
                "last_wake_word": "",
                "last_wake_word_url": "",
            },
        )
        matched_row.update(
            {
                "state": "linked",
                "trainer_name": safe_trainer_name,
                "linked_at": linked_at,
                "expires_at": max(float(matched_row.get("expires_at") or 0.0), time.time() + 60.0),
            }
        )
        _write_pairing(matched_id, matched_row)

    return {
        "ok": True,
        "linked": True,
        "token": link_token,
        "tater_name": "Tater",
        "linked_at": linked_at,
    }


def unlink() -> Dict[str, Any]:
    redis_client.delete(LINK_HASH_KEY)
    return {
        "ok": True,
        "message": "Wake Word Trainer unlinked.",
        **status(),
    }


def authorize(token: Any) -> Dict[str, Any]:
    supplied = _text(token)
    raw = _raw_status()
    expected_hash = _text(raw.get("token_hash"))
    if not supplied or not expected_hash:
        raise PermissionError("Wake Word Trainer is not linked.")
    if not hmac.compare_digest(_token_digest(supplied), expected_hash):
        raise PermissionError("Invalid Wake Word Trainer link token.")
    return raw


def _origin(value: str) -> tuple[str, str, int]:
    parsed = urlparse(value)
    scheme = str(parsed.scheme or "").lower()
    host = str(parsed.hostname or "").lower()
    port = parsed.port or (443 if scheme == "https" else 80)
    return scheme, host, int(port)


def validate_wake_word_url(value: Any, link: Dict[str, Any]) -> str:
    url = _text(value)
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Wake-word JSON URL must start with http:// or https://.")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("Wake-word JSON URL is invalid.")
    if not parsed.path.startswith("/api/trained_wake_words/") or not parsed.path.lower().endswith(".json"):
        raise ValueError("Linked trainers may only publish their trained wake-word JSON packages.")
    publish_base_url = _text(link.get("publish_base_url"))
    if not publish_base_url or _origin(url) != _origin(publish_base_url):
        raise ValueError("Wake-word JSON URL does not belong to the linked trainer.")
    return url


def record_publish(*, wake_word: Any, wake_word_url: Any) -> None:
    redis_client.hset(
        LINK_HASH_KEY,
        mapping={
            "last_publish_at": _iso_now(),
            "last_publish_ts": str(time.time()),
            "last_wake_word": _text(wake_word),
            "last_wake_word_url": _text(wake_word_url),
        },
    )
