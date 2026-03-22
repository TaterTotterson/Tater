import base64
import ipaddress
import json
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Dict, List, Optional

from helpers import redis_blob_client

MEDIA_TYPES = {"image", "audio", "video", "file"}
MEDIA_REF_PREFIX = "notifyq:media"
BLOB_PREFIX = "tater:blob:notify"
DEFAULT_TTL_SECONDS = 60 * 60 * 24 * 7
DEFAULT_URL_FETCH_MAX_BYTES = int(os.getenv("TATER_NOTIFY_MEDIA_URL_MAX_BYTES", "25000000"))
DEFAULT_URL_FETCH_TIMEOUT_SEC = int(os.getenv("TATER_NOTIFY_MEDIA_URL_TIMEOUT_SEC", "20"))


def _blob_client():
    return redis_blob_client


def _blob_key() -> str:
    return f"{BLOB_PREFIX}:{uuid.uuid4().hex}"


def _load_blob(blob_key: str) -> Optional[bytes]:
    if not blob_key:
        return None
    return _blob_client().get(blob_key.encode("utf-8"))


def _store_blob(binary: bytes, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> str:
    key = _blob_key()
    client = _blob_client()
    client.set(key.encode("utf-8"), binary)
    if ttl_seconds and ttl_seconds > 0:
        client.expire(key.encode("utf-8"), int(ttl_seconds))
    return key


def _coerce_bytes(value: Any) -> Optional[bytes]:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return None


def _decode_b64(value: str) -> Optional[bytes]:
    try:
        return base64.b64decode(value)
    except Exception:
        return None


def _read_path(path: str) -> Optional[bytes]:
    try:
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return None


def _coerce_size(value: Any) -> Optional[int]:
    try:
        n = int(value)
        if n >= 0:
            return n
    except Exception:
        pass
    return None


def _name_from_url(url: str) -> str:
    try:
        path = urllib.parse.urlparse(url).path or ""
    except Exception:
        path = ""
    name = os.path.basename(path).strip()
    return name


def _is_private_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    except Exception:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def _host_is_private(host: str) -> bool:
    host = str(host or "").strip()
    if not host:
        return True
    if _is_private_ip(host):
        return True
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        return True
    for info in infos:
        ip_str = info[4][0]
        if _is_private_ip(ip_str):
            return True
    return False


def _validate_remote_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(str(url or "").strip())
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    if not parsed.hostname:
        return False
    if _host_is_private(parsed.hostname):
        return False
    return True


def _read_url(url: str) -> tuple[Optional[bytes], Optional[str]]:
    if not _validate_remote_url(url):
        return None, None
    try:
        req = urllib.request.Request(
            str(url).strip(),
            headers={"User-Agent": "Tater-Notify/1.0"},
        )
        with urllib.request.urlopen(req, timeout=DEFAULT_URL_FETCH_TIMEOUT_SEC) as resp:
            raw = resp.read(DEFAULT_URL_FETCH_MAX_BYTES + 1)
            content_type = str(resp.headers.get("Content-Type") or "").strip()
        if len(raw) > DEFAULT_URL_FETCH_MAX_BYTES:
            return None, None
        mime = content_type.split(";", 1)[0].strip().lower()
        return raw, mime
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None, None
    except Exception:
        return None, None


def _normalize_media_item(item: Any, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    kind = str(item.get("type") or "").strip().lower()
    if kind not in MEDIA_TYPES:
        return None

    name = str(item.get("name") or f"{kind}.bin").strip()
    mimetype = str(item.get("mimetype") or "application/octet-stream").strip()
    size = _coerce_size(item.get("size"))
    remote_url = str(item.get("url") or "").strip()

    blob_key = str(item.get("blob_key") or "").strip()
    if blob_key and _load_blob(blob_key) is None:
        blob_key = ""

    if not blob_key:
        binary = _coerce_bytes(item.get("bytes"))
        if binary is None and isinstance(item.get("data"), str):
            binary = _decode_b64(item.get("data"))
        if binary is None and isinstance(item.get("path"), str):
            binary = _read_path(item.get("path"))
        remote_mimetype = ""
        if binary is None and remote_url:
            binary, remote_mimetype = _read_url(remote_url)
            if not name or name == f"{kind}.bin":
                guessed_name = _name_from_url(remote_url)
                if guessed_name:
                    name = guessed_name
            if remote_mimetype and (not mimetype or mimetype == "application/octet-stream"):
                mimetype = remote_mimetype

        if binary is not None:
            blob_key = _store_blob(binary, ttl_seconds=ttl_seconds)
            if size is None:
                size = len(binary)

    if not blob_key:
        return None

    out: Dict[str, Any] = {
        "type": kind,
        "name": name,
        "mimetype": mimetype,
        "blob_key": blob_key,
    }
    if size is not None:
        out["size"] = size
    return out


def media_refs_key(item_id: str) -> str:
    return f"{MEDIA_REF_PREFIX}:{item_id}"


def store_queue_attachments(
    redis_client,
    item_id: str,
    attachments: Any,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> int:
    item_id = str(item_id or "").strip()
    if not item_id:
        return 0

    normalized: List[Dict[str, Any]] = []
    if isinstance(attachments, list):
        for raw in attachments:
            media_item = _normalize_media_item(raw, ttl_seconds=ttl_seconds)
            if media_item:
                normalized.append(media_item)

    key = media_refs_key(item_id)
    if not normalized:
        try:
            redis_client.delete(key)
        except Exception:
            pass
        return 0

    redis_client.set(key, json.dumps(normalized))
    if ttl_seconds and ttl_seconds > 0:
        redis_client.expire(key, int(ttl_seconds))
    return len(normalized)


def load_queue_attachments(redis_client, item_id: Any) -> List[Dict[str, Any]]:
    item_id = str(item_id or "").strip()
    if not item_id:
        return []

    raw = redis_client.get(media_refs_key(item_id))
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return []

    if not isinstance(parsed, list):
        return []

    out: List[Dict[str, Any]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        kind = str(entry.get("type") or "").strip().lower()
        blob_key = str(entry.get("blob_key") or "").strip()
        if kind not in MEDIA_TYPES or not blob_key:
            continue
        out.append(entry)
    return out
