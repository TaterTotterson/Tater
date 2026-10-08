from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import threading
from typing import Any, Dict, Tuple
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen


MAX_MANIFEST_BYTES = 128 * 1024
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
    parsed = urlsplit(source)
    bundle_path = parsed.path.rsplit(".", 1)[0] + ".wake-bundle.json"
    bundle_source = parsed._replace(path=bundle_path, query="", fragment="").geturl()
    with _PACKAGES_LOCK:
        _PACKAGES[package_id] = {
            "manifest_url": source,
            "model_url": "",
            "bundle_url": bundle_source,
            "oww_metadata_url": "",
            "oww_model_url": "",
        }
    return f"{base}/api/tater/satellite/v1/wake-package/{package_id}/manifest.json"


def register_bundle(base_url: Any, source_url: Any) -> str:
    source = _validated_http_url(source_url, label="openWakeWord bundle URL")
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("Tater service URL is unavailable")
    package_id = _package_id(source)
    with _PACKAGES_LOCK:
        _PACKAGES[package_id] = {
            "manifest_url": "",
            "model_url": "",
            "bundle_url": source,
            "oww_metadata_url": "",
            "oww_model_url": "",
        }
    return f"{base}/api/tater/satellite/v1/wake-package/{package_id}/bundle.wake-bundle.json"


def register_dual_bundle(base_url: Any, source_url: Any) -> Tuple[str, str]:
    bundle_url = register_bundle(base_url, source_url)
    package_id = urlsplit(bundle_url).path.rstrip("/").split("/")[-2]
    base = str(base_url or "").strip().rstrip("/")
    manifest_url = f"{base}/api/tater/satellite/v1/wake-package/{package_id}/manifest.json"
    return manifest_url, bundle_url


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
    manifest_url = str(row.get("manifest_url") or "").strip()
    if not manifest_url and row.get("bundle_url"):
        bundle_bytes(token)
        row = _package(token)
        manifest_url = str(row.get("manifest_url") or "").strip()
    if not manifest_url:
        raise WakePackageProxyError("Wake package manifest URL is unavailable")
    raw, final_manifest_url = _download(manifest_url, MAX_MANIFEST_BYTES)
    expected_manifest_sha = str(row.get("mww_manifest_sha256") or "").strip().lower()
    if expected_manifest_sha and not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_manifest_sha):
        raise WakePackageProxyError("microWakeWord manifest does not match the selected dual-model bundle")
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
    expected_model_url = str(row.get("mww_model_url") or "").strip()
    if expected_model_url and model_url != expected_model_url:
        raise WakePackageProxyError("microWakeWord manifest names a model outside the selected dual-model bundle")
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
    expected_sha = str(row.get("mww_model_sha256") or "").strip().lower()
    if expected_sha and not hmac.compare_digest(hashlib.sha256(body).hexdigest(), expected_sha):
        raise WakePackageProxyError("microWakeWord model does not match the selected dual-model bundle")
    return body


def bundle_bytes(package_id: Any) -> bytes:
    token = str(package_id or "").strip().lower()
    row = _package(token)
    bundle_url = str(row.get("bundle_url") or "").strip()
    if not bundle_url:
        raise WakePackageProxyError("openWakeWord bundle URL is unavailable")
    raw, final_bundle_url = _download(bundle_url, MAX_MANIFEST_BYTES)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WakePackageProxyError(f"openWakeWord bundle is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise WakePackageProxyError("openWakeWord bundle must contain a JSON object")
    if payload.get("schema_version") != 1 or payload.get("type") != "tater_wake_word_bundle":
        raise WakePackageProxyError("Wake bundle schema is unsupported")
    micro = payload.get("micro_wake_word")
    oww = payload.get("open_wake_word")
    if not isinstance(micro, dict) or not isinstance(oww, dict):
        raise WakePackageProxyError("Wake bundle must contain matching micro_wake_word and open_wake_word models")
    manifest_ref = str(micro.get("manifest") or "").strip()
    mww_model_ref = str(micro.get("model") or "").strip()
    manifest_sha = str(micro.get("manifest_sha256") or "").strip().lower()
    mww_model_sha = str(micro.get("model_sha256") or "").strip().lower()
    metadata_ref = str(oww.get("metadata") or "").strip()
    metadata_sha = str(oww.get("metadata_sha256") or "").strip().lower()
    artifacts = oww.get("artifacts")
    digests = (manifest_sha, mww_model_sha, metadata_sha)
    if (
        not manifest_ref
        or not mww_model_ref
        or not metadata_ref
        or not isinstance(artifacts, dict)
        or any(len(value) != 64 or any(character not in "0123456789abcdef" for character in value) for value in digests)
    ):
        raise WakePackageProxyError("Wake bundle does not contain complete, verified dual-model artifacts")
    classifier_key = ""
    classifier = artifacts.get("onnx")
    if isinstance(classifier, dict):
        classifier_key = "onnx"
    else:
        for key, candidate in artifacts.items():
            if isinstance(candidate, dict) and str(candidate.get("file") or "").strip().lower().endswith(".onnx"):
                if classifier_key:
                    raise WakePackageProxyError("Wake bundle contains multiple ONNX classifiers")
                classifier_key = str(key)
                classifier = candidate
    if not classifier_key or not isinstance(classifier, dict):
        raise WakePackageProxyError("Wake bundle does not contain an ONNX classifier")
    classifier_ref = str(classifier.get("file") or "").strip()
    classifier_sha = str(classifier.get("sha256") or "").strip().lower()
    if len(classifier_sha) != 64 or any(character not in "0123456789abcdef" for character in classifier_sha):
        raise WakePackageProxyError("Wake bundle openWakeWord classifier digest is invalid")
    manifest_url = _validated_http_url(urljoin(final_bundle_url, manifest_ref), label="microWakeWord manifest URL")
    mww_model_url = _validated_http_url(urljoin(final_bundle_url, mww_model_ref), label="microWakeWord model URL")
    metadata_url = _validated_http_url(urljoin(final_bundle_url, metadata_ref), label="openWakeWord metadata URL")
    model_url = _validated_http_url(urljoin(final_bundle_url, classifier_ref), label="openWakeWord model URL")
    with _PACKAGES_LOCK:
        current = _PACKAGES.get(token)
        if isinstance(current, dict):
            current["manifest_url"] = manifest_url
            current["mww_model_url"] = mww_model_url
            current["mww_manifest_sha256"] = manifest_sha
            current["mww_model_sha256"] = mww_model_sha
            current["oww_metadata_url"] = metadata_url
            current["oww_model_url"] = model_url
            current["oww_metadata_sha256"] = metadata_sha
            current["oww_model_sha256"] = classifier_sha
    micro = dict(micro)
    micro["manifest"] = "manifest.json"
    micro["model"] = "model.tflite"
    # The LAN proxy rewrites the MWW manifest's model reference to its local
    # route. Its bytes therefore differ from the trainer copy, so publish the
    # hash of those exact proxied bytes in the proxied bundle. The model bytes
    # themselves are unchanged and retain the trainer's digest.
    proxied_manifest = manifest_bytes(token)
    micro["manifest_sha256"] = hashlib.sha256(proxied_manifest).hexdigest()
    oww = dict(oww)
    oww["metadata"] = "oww-metadata.json"
    rewritten = dict(classifier)
    rewritten["file"] = "oww-model.onnx"
    oww["artifacts"] = {"onnx": rewritten}
    payload = dict(payload)
    payload["micro_wake_word"] = micro
    payload["open_wake_word"] = oww
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > MAX_MANIFEST_BYTES:
        raise WakePackageProxyError(f"Wake bundle exceeds {MAX_MANIFEST_BYTES} bytes")
    return encoded


def oww_metadata_bytes(package_id: Any) -> bytes:
    token = str(package_id or "").strip().lower()
    row = _package(token)
    metadata_url = str(row.get("oww_metadata_url") or "").strip()
    if not metadata_url:
        bundle_bytes(token)
        row = _package(token)
        metadata_url = str(row.get("oww_metadata_url") or "").strip()
    body, _ = _download(metadata_url, MAX_MANIFEST_BYTES)
    expected_sha = str(row.get("oww_metadata_sha256") or "").strip().lower()
    if expected_sha and not hmac.compare_digest(hashlib.sha256(body).hexdigest(), expected_sha):
        raise WakePackageProxyError("openWakeWord metadata does not match the selected dual-model bundle")
    return body


def oww_model_bytes(package_id: Any) -> bytes:
    token = str(package_id or "").strip().lower()
    row = _package(token)
    model_url = str(row.get("oww_model_url") or "").strip()
    if not model_url:
        bundle_bytes(token)
        row = _package(token)
        model_url = str(row.get("oww_model_url") or "").strip()
    body, _ = _download(model_url, MAX_MODEL_BYTES)
    # ONNX ModelProto requires field 1 (ir_version), encoded as protobuf tag
    # 0x08. The bundle digest remains the authoritative integrity check.
    if len(body) < 2 or body[0] != 0x08:
        raise WakePackageProxyError("openWakeWord model is not an ONNX ModelProto")
    expected_sha = str(row.get("oww_model_sha256") or "").strip().lower()
    if expected_sha and not hmac.compare_digest(hashlib.sha256(body).hexdigest(), expected_sha):
        raise WakePackageProxyError("openWakeWord model does not match the selected dual-model bundle")
    return body
