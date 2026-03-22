import base64
import json
import mimetypes
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional

from helpers import redis_blob_client


CONVERSATION_ARTIFACT_PREFIX = "tater:conversation_artifacts"
CONVERSATION_ARTIFACT_SEQ_PREFIX = "tater:conversation_artifact_seq"
CONVERSATION_ARTIFACT_BLOB_PREFIX = "tater:blob:conversation_artifact"
DEFAULT_ARTIFACT_TTL_SEC = int(
    os.getenv("TATER_CONVERSATION_ARTIFACT_TTL_SEC", str(60 * 60 * 24 * 14))
)
DEFAULT_ARTIFACT_MAX_ITEMS = int(
    os.getenv("TATER_CONVERSATION_ARTIFACT_MAX_ITEMS", "64")
)
_ARTIFACT_TYPES = {"image", "audio", "video", "file"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _platform_token(platform: Any) -> str:
    raw = _clean(platform).lower() or "unknown"
    return re.sub(r"[^a-z0-9_.:\-]+", "_", raw).strip("_") or "unknown"


def _scope_token(scope: Any) -> str:
    raw = _clean(scope).lower() or "default"
    return re.sub(r"[^a-z0-9_.:\-!@#]+", "_", raw).strip("_") or "default"


def artifacts_key(platform: Any, scope: Any) -> str:
    return f"{CONVERSATION_ARTIFACT_PREFIX}:{_platform_token(platform)}:{_scope_token(scope)}"


def artifact_seq_key(platform: Any, scope: Any) -> str:
    return f"{CONVERSATION_ARTIFACT_SEQ_PREFIX}:{_platform_token(platform)}:{_scope_token(scope)}"


def _blob_client():
    return redis_blob_client


def _blob_key() -> str:
    return f"{CONVERSATION_ARTIFACT_BLOB_PREFIX}:{uuid.uuid4().hex}"


def _store_blob(binary: bytes, ttl_seconds: int = DEFAULT_ARTIFACT_TTL_SEC) -> str:
    key = _blob_key()
    client = _blob_client()
    client.set(key.encode("utf-8"), binary)
    if ttl_seconds and ttl_seconds > 0:
        client.expire(key.encode("utf-8"), int(ttl_seconds))
    return key


def _artifact_type_from_mimetype(mimetype: Any) -> str:
    mime = _clean(mimetype).lower()
    if mime.startswith("image/"):
        return "image"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith("video/"):
        return "video"
    return "file"


def _coerce_size(value: Any) -> Optional[int]:
    try:
        size_value = int(value)
    except Exception:
        return None
    if size_value < 0:
        return None
    return size_value


def _decode_base64_data(data: Any) -> Optional[bytes]:
    text = _clean(data)
    if not text:
        return None
    if text.startswith("data:") and "," in text:
        text = text.split(",", 1)[1]
    pad = len(text) % 4
    if pad:
        text += "=" * (4 - pad)
    try:
        decoded = base64.b64decode(text)
    except Exception:
        return None
    return bytes(decoded) if decoded else None


def _artifact_name(path: str, fallback: str) -> str:
    raw = str(path or "").strip().replace("\\", "/")
    if raw and "/" in raw:
        tail = raw.rsplit("/", 1)[-1].strip()
        if tail:
            return tail
    return fallback


def _artifact_identity(item: Dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        _clean(item.get("type")).lower(),
        _clean(item.get("path")),
        _clean(item.get("blob_key")),
        _clean(item.get("file_id")),
        _clean(item.get("url")),
    )


def _load_rows(redis_client: Any, platform: Any, scope: Any) -> List[Dict[str, Any]]:
    key = artifacts_key(platform, scope)
    try:
        raw = redis_client.get(key)
    except Exception:
        return []
    if not raw:
        return []
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", errors="ignore")
        except Exception:
            return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in parsed:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def _normalize_artifact(
    artifact: Any,
    *,
    ttl_sec: int,
) -> Optional[Dict[str, Any]]:
    if not isinstance(artifact, dict):
        return None

    artifact_id = _clean(artifact.get("artifact_id"))
    path = _clean(artifact.get("path"))
    blob_key = _clean(artifact.get("blob_key"))
    file_id = _clean(artifact.get("file_id") or artifact.get("id"))
    url = _clean(artifact.get("url"))
    name = _clean(artifact.get("name"))
    mimetype_value = _clean(artifact.get("mimetype")).lower()
    source = _clean(artifact.get("source")) or "artifact"
    artifact_type = _clean(artifact.get("type")).lower()

    binary: Optional[bytes] = None
    if isinstance(artifact.get("bytes"), (bytes, bytearray)):
        binary = bytes(artifact.get("bytes"))
    elif isinstance(artifact.get("data"), (bytes, bytearray)):
        binary = bytes(artifact.get("data"))
    elif isinstance(artifact.get("data"), str):
        binary = _decode_base64_data(artifact.get("data"))

    if binary:
        blob_key = _store_blob(binary, ttl_seconds=ttl_sec)
        if not name:
            name = "file.bin"
        if not mimetype_value:
            mimetype_value = (
                str(mimetypes.guess_type(name)[0] or "").strip().lower()
                or "application/octet-stream"
            )

    if not any((path, blob_key, file_id, url)):
        return None

    if not name:
        if path:
            name = _artifact_name(path, "file.bin")
        elif url:
            name = _artifact_name(url, "file.bin")
        else:
            name = "file.bin"

    if not mimetype_value:
        guessed = str(mimetypes.guess_type(name or path)[0] or "").strip().lower()
        mimetype_value = guessed or "application/octet-stream"

    if artifact_type not in _ARTIFACT_TYPES:
        artifact_type = _artifact_type_from_mimetype(mimetype_value)

    out: Dict[str, Any] = {
        "artifact_id": artifact_id,
        "type": artifact_type,
        "name": name,
        "mimetype": mimetype_value,
        "source": source,
        "stored_at": time.time(),
    }
    for key, value in (("path", path), ("blob_key", blob_key), ("file_id", file_id), ("url", url)):
        if value:
            out[key] = value

    size_value = _coerce_size(artifact.get("size"))
    if size_value is None and binary is not None:
        size_value = len(binary)
    if size_value is not None:
        out["size"] = size_value
    return out


def _next_artifact_id(redis_client: Any, platform: Any, scope: Any) -> str:
    key = artifact_seq_key(platform, scope)
    try:
        seq = int(redis_client.incr(key))
    except Exception:
        seq = int(time.time() * 1000) % 1_000_000
    return f"att{seq}"


def save_conversation_artifact(
    redis_client: Any,
    *,
    platform: Any,
    scope: Any,
    artifact: Dict[str, Any],
    ttl_sec: Optional[int] = None,
    max_items: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    ttl_value = DEFAULT_ARTIFACT_TTL_SEC if ttl_sec is None else max(0, int(ttl_sec))
    keep = DEFAULT_ARTIFACT_MAX_ITEMS if max_items is None else max(1, int(max_items))
    normalized = _normalize_artifact(artifact, ttl_sec=ttl_value)
    if normalized is None:
        return None

    rows = _load_rows(redis_client, platform, scope)
    identity = _artifact_identity(normalized)
    preserved_id = ""
    remaining: List[Dict[str, Any]] = []
    for item in rows:
        if _artifact_identity(item) == identity:
            preserved_id = _clean(item.get("artifact_id"))
            continue
        remaining.append(item)

    normalized["artifact_id"] = preserved_id or _clean(normalized.get("artifact_id")) or _next_artifact_id(redis_client, platform, scope)
    payload = [normalized] + remaining
    payload = payload[:keep]
    key = artifacts_key(platform, scope)
    try:
        redis_client.set(key, json.dumps(payload, ensure_ascii=False))
        if ttl_value > 0:
            redis_client.expire(key, ttl_value)
    except Exception:
        return normalized
    return normalized


def save_conversation_artifacts(
    redis_client: Any,
    *,
    platform: Any,
    scope: Any,
    artifacts: Any,
    ttl_sec: Optional[int] = None,
    max_items: Optional[int] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(artifacts, list):
        return out
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        saved = save_conversation_artifact(
            redis_client,
            platform=platform,
            scope=scope,
            artifact=artifact,
            ttl_sec=ttl_sec,
            max_items=max_items,
        )
        if saved is not None:
            out.append(saved)
    return out


def load_conversation_artifacts(
    redis_client: Any,
    *,
    platform: Any,
    scope: Any,
    limit: int = 16,
) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    rows = _load_rows(redis_client, platform, scope)
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in rows:
        normalized = _normalize_artifact(item, ttl_sec=DEFAULT_ARTIFACT_TTL_SEC)
        if normalized is None:
            continue
        artifact_id = _clean(item.get("artifact_id"))
        if artifact_id:
            normalized["artifact_id"] = artifact_id
        dedupe_key = (
            _clean(normalized.get("artifact_id")),
            _artifact_identity(normalized),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(normalized)
        if len(out) >= limit:
            break
    return out
