from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import threading
from typing import Any, Dict, Tuple
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen


MAX_MANIFEST_BYTES = 512 * 1024
MAX_MODEL_BYTES = 16 * 1024 * 1024
DOWNLOAD_TIMEOUT_S = 20.0

_SECRET = secrets.token_bytes(32)
_PACKAGES_LOCK = threading.RLock()
_PACKAGES: Dict[str, Dict[str, Any]] = {}
_PACKAGE_FETCH_LOCKS: Dict[str, threading.RLock] = {}


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


def _fetch_lock(package_id: str) -> threading.RLock:
    with _PACKAGES_LOCK:
        return _PACKAGE_FETCH_LOCKS.setdefault(package_id, threading.RLock())


def _register_package(package_id: str, values: Dict[str, str]) -> None:
    """Register source URLs without discarding already fetched artifacts."""
    with _PACKAGES_LOCK:
        row = _PACKAGES.setdefault(package_id, {})
        for key, value in values.items():
            if value or key not in row:
                row[key] = value


def _artifact_bytes(package_id: str, field: str, loader: Any) -> bytes:
    # One slow upstream fetch populates the package for every satellite that
    # reconnects at once. The per-package RLock also permits bundle loading to
    # resolve its nested manifest without deadlocking.
    with _fetch_lock(package_id):
        row = _package(package_id)
        cached = row.get(field)
        if isinstance(cached, bytes):
            return cached
        body = loader()
        with _PACKAGES_LOCK:
            current = _PACKAGES.get(package_id)
            if isinstance(current, dict):
                current[field] = body
        return body


def register_manifest(base_url: Any, source_url: Any) -> str:
    source = _validated_http_url(source_url, label="Wake package URL")
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("Tater service URL is unavailable")
    if urlsplit(source).path.lower().endswith(".wake-bundle.json"):
        bundle_url = register_bundle(base, source)
        package_id = urlsplit(bundle_url).path.rstrip("/").split("/")[-2]
        return f"{base}/api/tater/satellite/v1/wake-package/{package_id}/manifest.json"
    package_id = _package_id(source)
    parsed = urlsplit(source)
    bundle_path = parsed.path.rsplit(".", 1)[0] + ".wake-bundle.json"
    bundle_source = parsed._replace(path=bundle_path, query="", fragment="").geturl()
    _register_package(
        package_id,
        {
            "manifest_url": source,
            "model_url": "",
            "bundle_url": bundle_source,
            "oww_metadata_url": "",
            "oww_model_url": "",
            "nww_model_urls": {},
        },
    )
    return f"{base}/api/tater/satellite/v1/wake-package/{package_id}/manifest.json"


def register_bundle(base_url: Any, source_url: Any) -> str:
    source = _validated_http_url(source_url, label="openWakeWord bundle URL")
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("Tater service URL is unavailable")
    package_id = _package_id(source)
    _register_package(
        package_id,
        {
            "manifest_url": "",
            "model_url": "",
            "bundle_url": source,
            "oww_metadata_url": "",
            "oww_model_url": "",
            "nww_model_urls": {},
        },
    )
    return f"{base}/api/tater/satellite/v1/wake-package/{package_id}/bundle.wake-bundle.json"


def register_dual_bundle(base_url: Any, source_url: Any) -> Tuple[str, str]:
    bundle_url = register_bundle(base_url, source_url)
    package_id = urlsplit(bundle_url).path.rstrip("/").split("/")[-2]
    base = str(base_url or "").strip().rstrip("/")
    manifest_url = f"{base}/api/tater/satellite/v1/wake-package/{package_id}/manifest.json"
    return manifest_url, bundle_url


def _package(package_id: Any) -> Dict[str, Any]:
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
    return _artifact_bytes(token, "manifest_body", lambda: _manifest_bytes_uncached(token))


def _manifest_bytes_uncached(token: str) -> bytes:
    row = _package(token)
    manifest_url = str(row.get("manifest_url") or "").strip()
    embedded = row.get("mww_manifest_payload")
    if not manifest_url and isinstance(embedded, dict):
        payload = dict(embedded)
        payload["model"] = "model.tflite"
        for key in ("manifest", "manifest_sha256", "model_sha256", "model_size_bytes"):
            payload.pop(key, None)
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(encoded) > MAX_MANIFEST_BYTES:
            raise WakePackageProxyError(f"Wake package manifest exceeds {MAX_MANIFEST_BYTES} bytes")
        return encoded
    if not manifest_url and row.get("bundle_url"):
        bundle_bytes(token)
        row = _package(token)
        cached = row.get("manifest_body")
        if isinstance(cached, bytes):
            return cached
        manifest_url = str(row.get("manifest_url") or "").strip()
        embedded = row.get("mww_manifest_payload")
    if not manifest_url and isinstance(embedded, dict):
        payload = dict(embedded)
        payload["model"] = "model.tflite"
        for key in ("manifest", "manifest_sha256", "model_sha256", "model_size_bytes"):
            payload.pop(key, None)
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(encoded) > MAX_MANIFEST_BYTES:
            raise WakePackageProxyError(f"Wake package manifest exceeds {MAX_MANIFEST_BYTES} bytes")
        return encoded
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
    return _artifact_bytes(token, "model_body", lambda: _model_bytes_uncached(token))


def _model_bytes_uncached(token: str) -> bytes:
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
    return _artifact_bytes(token, "bundle_body", lambda: _bundle_bytes_uncached(token))


def _bundle_bytes_uncached(token: str) -> bytes:
    row = _package(token)
    bundle_url = str(row.get("bundle_url") or "").strip()
    if not bundle_url:
        raise WakePackageProxyError("Wake bundle URL is unavailable")
    raw, final_bundle_url = _download(bundle_url, MAX_MANIFEST_BYTES)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WakePackageProxyError(f"Wake bundle is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise WakePackageProxyError("Wake bundle must contain a JSON object")
    if payload.get("schema_version") not in {1, 2, 3} or payload.get("type") != "tater_wake_word_bundle":
        raise WakePackageProxyError("Wake bundle schema is unsupported")
    micro = payload.get("micro_wake_word")
    oww = payload.get("open_wake_word")
    nano = payload.get("nano_wake_word")
    if not isinstance(micro, dict):
        raise WakePackageProxyError("Wake bundle must contain a micro_wake_word fallback")
    oww = dict(oww) if isinstance(oww, dict) else {}
    nano = dict(nano) if isinstance(nano, dict) else {}

    def valid_digest(value: str) -> bool:
        return len(value) == 64 and all(character in "0123456789abcdef" for character in value)

    manifest_ref = str(micro.get("manifest") or "").strip()
    mww_model_ref = str(micro.get("model") or "").strip()
    manifest_sha = str(micro.get("manifest_sha256") or "").strip().lower()
    mww_model_sha = str(micro.get("model_sha256") or "").strip().lower()
    if (
        not mww_model_ref.lower().endswith(".tflite")
        or not valid_digest(mww_model_sha)
        or (manifest_ref and (not manifest_ref.lower().endswith(".json") or not valid_digest(manifest_sha)))
    ):
        raise WakePackageProxyError("Wake bundle does not contain a verified microWakeWord fallback")

    metadata_ref = str(oww.get("metadata") or "").strip()
    metadata_sha = str(oww.get("metadata_sha256") or "").strip().lower()
    artifacts = oww.get("artifacts") if isinstance(oww.get("artifacts"), dict) else {}
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
    classifier_ref = str(classifier.get("file") or "").strip() if isinstance(classifier, dict) else ""
    classifier_sha = str(classifier.get("sha256") or "").strip().lower() if isinstance(classifier, dict) else ""
    if classifier_ref and not valid_digest(classifier_sha):
        raise WakePackageProxyError("Wake bundle openWakeWord classifier digest is invalid")
    if metadata_ref and not valid_digest(metadata_sha):
        raise WakePackageProxyError("Wake bundle openWakeWord metadata digest is invalid")

    manifest_url = (
        _validated_http_url(urljoin(final_bundle_url, manifest_ref), label="microWakeWord manifest URL")
        if manifest_ref else ""
    )
    mww_model_url = _validated_http_url(urljoin(final_bundle_url, mww_model_ref), label="microWakeWord model URL")
    metadata_url = (
        _validated_http_url(urljoin(final_bundle_url, metadata_ref), label="openWakeWord metadata URL")
        if metadata_ref else ""
    )
    model_url = (
        _validated_http_url(urljoin(final_bundle_url, classifier_ref), label="openWakeWord model URL")
        if classifier_ref else ""
    )

    variants = nano.get("variants") if isinstance(nano.get("variants"), dict) else {}
    rewritten_variants: Dict[str, Any] = {}
    nww_model_urls: Dict[str, str] = {}
    nww_model_sha256: Dict[str, str] = {}
    nww_model_sizes: Dict[str, int] = {}
    for architecture in ("cnn", "gru"):
        variant = variants.get(architecture)
        if not isinstance(variant, dict):
            continue
        model_ref = str(variant.get("model") or "").strip()
        model_sha = str(variant.get("model_sha256") or "").strip().lower()
        model_size_value = variant.get("model_size_bytes")
        if not model_ref:
            variant_artifacts = variant.get("artifacts") if isinstance(variant.get("artifacts"), dict) else {}
            variant_model = variant_artifacts.get("onnx")
            if isinstance(variant_model, dict):
                model_ref = str(variant_model.get("file") or "").strip()
                model_sha = str(variant_model.get("sha256") or "").strip().lower()
                model_size_value = variant_model.get("size_bytes")
        try:
            model_size = int(model_size_value or 0)
        except (TypeError, ValueError):
            model_size = 0
        if not model_ref.lower().endswith(".onnx") or not valid_digest(model_sha) or not (0 < model_size <= MAX_MODEL_BYTES):
            raise WakePackageProxyError(f"Wake bundle NanoWakeWord {architecture.upper()} model is invalid")
        nww_model_urls[architecture] = _validated_http_url(
            urljoin(final_bundle_url, model_ref),
            label=f"NanoWakeWord {architecture.upper()} model URL",
        )
        nww_model_sha256[architecture] = model_sha
        nww_model_sizes[architecture] = model_size
        rewritten = dict(variant)
        rewritten["model"] = f"nww-{architecture}-model.onnx"
        rewritten["model_sha256"] = model_sha
        rewritten["model_size_bytes"] = model_size
        rewritten.pop("artifacts", None)
        rewritten_variants[architecture] = rewritten

    with _PACKAGES_LOCK:
        current = _PACKAGES.get(token)
        if isinstance(current, dict):
            current["manifest_url"] = manifest_url
            current["mww_manifest_payload"] = dict(micro) if not manifest_url else None
            current["model_url"] = mww_model_url
            current["mww_model_url"] = mww_model_url
            current["mww_manifest_sha256"] = manifest_sha
            current["mww_model_sha256"] = mww_model_sha
            current["oww_metadata_url"] = metadata_url
            current["oww_model_url"] = model_url
            current["oww_metadata_sha256"] = metadata_sha
            current["oww_model_sha256"] = classifier_sha
            current["nww_model_urls"] = nww_model_urls
            current["nww_model_sha256"] = nww_model_sha256
            current["nww_model_sizes"] = nww_model_sizes
    micro = dict(micro)
    micro["manifest"] = "manifest.json"
    micro["model"] = "model.tflite"
    # The LAN proxy rewrites the MWW manifest's model reference to its local
    # route. Its bytes therefore differ from the trainer copy, so publish the
    # hash of those exact proxied bytes in the proxied bundle. The model bytes
    # themselves are unchanged and retain the trainer's digest.
    proxied_manifest = manifest_bytes(token)
    micro["manifest_sha256"] = hashlib.sha256(proxied_manifest).hexdigest()
    if metadata_ref:
        oww["metadata"] = "oww-metadata.json"
    if classifier_ref and isinstance(classifier, dict):
        rewritten = dict(classifier)
        rewritten["file"] = "oww-model.onnx"
        oww["artifacts"] = {"onnx": rewritten}
    if rewritten_variants:
        nano["variants"] = rewritten_variants
    payload = dict(payload)
    payload["micro_wake_word"] = micro
    if oww:
        payload["open_wake_word"] = oww
    if nano:
        payload["nano_wake_word"] = nano
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > MAX_MANIFEST_BYTES:
        raise WakePackageProxyError(f"Wake bundle exceeds {MAX_MANIFEST_BYTES} bytes")
    return encoded


def oww_metadata_bytes(package_id: Any) -> bytes:
    token = str(package_id or "").strip().lower()
    return _artifact_bytes(token, "oww_metadata_body", lambda: _oww_metadata_bytes_uncached(token))


def _oww_metadata_bytes_uncached(token: str) -> bytes:
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
    return _artifact_bytes(token, "oww_model_body", lambda: _oww_model_bytes_uncached(token))


def _oww_model_bytes_uncached(token: str) -> bytes:
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


def nww_model_bytes(package_id: Any, architecture: Any) -> bytes:
    token = str(package_id or "").strip().lower()
    model_type = str(architecture or "").strip().lower()
    if model_type not in {"cnn", "gru"}:
        raise WakePackageProxyError("Unsupported NanoWakeWord architecture")
    return _artifact_bytes(
        token,
        f"nww_{model_type}_model_body",
        lambda: _nww_model_bytes_uncached(token, model_type),
    )


def _nww_model_bytes_uncached(token: str, architecture: str) -> bytes:
    row = _package(token)
    urls = row.get("nww_model_urls") if isinstance(row.get("nww_model_urls"), dict) else {}
    model_url = str(urls.get(architecture) or "").strip()
    if not model_url:
        bundle_bytes(token)
        row = _package(token)
        urls = row.get("nww_model_urls") if isinstance(row.get("nww_model_urls"), dict) else {}
        model_url = str(urls.get(architecture) or "").strip()
    if not model_url:
        raise WakePackageProxyError(f"NanoWakeWord {architecture.upper()} model is unavailable")
    body, _ = _download(model_url, MAX_MODEL_BYTES)
    if len(body) < 2 or body[0] != 0x08:
        raise WakePackageProxyError("NanoWakeWord model is not an ONNX ModelProto")
    hashes = row.get("nww_model_sha256") if isinstance(row.get("nww_model_sha256"), dict) else {}
    expected_sha = str(hashes.get(architecture) or "").strip().lower()
    sizes = row.get("nww_model_sizes") if isinstance(row.get("nww_model_sizes"), dict) else {}
    try:
        expected_size = int(sizes.get(architecture) or 0)
    except (TypeError, ValueError):
        expected_size = 0
    if expected_size and len(body) != expected_size:
        raise WakePackageProxyError("NanoWakeWord model size does not match the selected wake bundle")
    if expected_sha and not hmac.compare_digest(hashlib.sha256(body).hexdigest(), expected_sha):
        raise WakePackageProxyError("NanoWakeWord model does not match the selected wake bundle")
    return body
