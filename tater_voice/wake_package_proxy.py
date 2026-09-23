from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import threading
from typing import Any, Dict, Tuple
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen


MAX_MANIFEST_BYTES = 64 * 1024
MAX_MODEL_BYTES = 16 * 1024 * 1024
DOWNLOAD_TIMEOUT_S = 20.0

_SECRET = secrets.token_bytes(32)
_PACKAGES_LOCK = threading.RLock()
_PACKAGES: Dict[str, Dict[str, str]] = {}


class WakePackageProxyError(RuntimeError):
    pass


def _validated_http_url(value: Any, *, label: str) -> str:
    token = str(value or "").strip()
    parsed = urlsplit(token)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{label} must use http:// or https://")
    return token


def _package_id(source_url: str) -> str:
    return hmac.new(_SECRET, source_url.encode("utf-8"), hashlib.sha256).hexdigest()[:32]


def register_manifest(base_url: Any, source_url: Any) -> str:
    source = _validated_http_url(source_url, label="Wake package URL")
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("Tater service URL is unavailable")
    package_id = _package_id(source)
    with _PACKAGES_LOCK:
        _PACKAGES[package_id] = {"manifest_url": source, "model_url": ""}
    return f"{base}/api/tater/satellite/v1/wake-package/{package_id}/manifest.json"


def _package(package_id: Any) -> Dict[str, str]:
    token = str(package_id or "").strip().lower()
    if len(token) != 32 or any(character not in "0123456789abcdef" for character in token):
        raise KeyError("Unknown wake package")
    with _PACKAGES_LOCK:
        row = _PACKAGES.get(token)
        if not isinstance(row, dict):
            raise KeyError("Unknown wake package")
        return dict(row)


def _download(raw_url: str, limit: int) -> Tuple[bytes, str]:
    url = _validated_http_url(raw_url, label="Wake package download URL")
    request = Request(url, headers={"User-Agent": "Tater-Wake-Package-Proxy/1.0"})
    try:
        with urlopen(request, timeout=DOWNLOAD_TIMEOUT_S) as response:
            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    if int(content_length) > limit:
                        raise WakePackageProxyError(f"Wake package download exceeds {limit} bytes")
                except ValueError:
                    pass
            body = response.read(limit + 1)
            final_url = response.geturl()
    except WakePackageProxyError:
        raise
    except Exception as exc:
        raise WakePackageProxyError(f"Could not download wake package: {exc}") from exc
    if len(body) > limit:
        raise WakePackageProxyError(f"Wake package download exceeds {limit} bytes")
    return body, _validated_http_url(final_url, label="Wake package redirect URL")


def manifest_bytes(package_id: Any) -> bytes:
    token = str(package_id or "").strip().lower()
    row = _package(token)
    raw, final_manifest_url = _download(row["manifest_url"], MAX_MANIFEST_BYTES)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WakePackageProxyError(f"Wake package manifest is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise WakePackageProxyError("Wake package manifest must contain a JSON object")
    model_reference = str(payload.get("model") or payload.get("model_url") or "").strip()
    if not model_reference:
        raise WakePackageProxyError("Wake package manifest does not name a model")
    model_url = _validated_http_url(
        urljoin(final_manifest_url, model_reference),
        label="Wake package model URL",
    )
    with _PACKAGES_LOCK:
        current = _PACKAGES.get(token)
        if isinstance(current, dict):
            current["model_url"] = model_url
    payload["model"] = "model.tflite"
    payload.pop("model_url", None)
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > MAX_MANIFEST_BYTES:
        raise WakePackageProxyError(f"Wake package manifest exceeds {MAX_MANIFEST_BYTES} bytes")
    return encoded


def model_bytes(package_id: Any) -> bytes:
    token = str(package_id or "").strip().lower()
    row = _package(token)
    model_url = str(row.get("model_url") or "").strip()
    if not model_url:
        manifest_bytes(token)
        row = _package(token)
        model_url = str(row.get("model_url") or "").strip()
    body, _ = _download(model_url, MAX_MODEL_BYTES)
    if len(body) < 8 or body[4:8] != b"TFL3":
        raise WakePackageProxyError("Wake package model is not a TFLite FlatBuffer")
    return body
