"""Isolated YuNet + SFace worker using OpenCV's reference ONNX pipeline."""

from __future__ import annotations

from tater._compat import expose_legacy as _expose_legacy
_expose_legacy(__name__)

import argparse
import base64
import hashlib
import json
import os
import sys
import tempfile
import traceback
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

from tater.identity.model import (
    DETECTOR_BACKEND, EMBEDDING_DIMENSIONS, MODEL_ASSETS, MODEL_ID,
    MODEL_NAME, MODEL_REVISION, require_model,
)

RESULT_PREFIX = "TATER_FACE_RESULT:"
MIN_FACE_SIDE = 32
MIN_FACE_AREA = 1600
DETECTION_CONFIDENCE = 0.65
_MODELS: Any = None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _number(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    return max(float(minimum), min(float(maximum), parsed))


def _integer(value: Any, default: int, minimum: int, maximum: int) -> int:
    return int(_number(value, default, minimum, maximum))


def _model_file(asset: Dict[str, str]) -> Path:
    from tater_paths import runtime_dir

    root = Path(os.environ.get("TATER_FACE_MODEL_DIR") or runtime_dir() / "models" / "face-id")
    root.mkdir(parents=True, exist_ok=True)
    path = root / asset["filename"]
    if path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == asset["sha256"]:
        return path
    url = (
        f"https://media.githubusercontent.com/media/opencv/opencv_zoo/{MODEL_REVISION}"
        f"/models/{asset['directory']}/{asset['filename']}"
    )
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=root, suffix=".download", delete=False) as output:
            temporary = Path(output.name)
            digest = hashlib.sha256()
            with urllib.request.urlopen(url, timeout=120) as response:
                while chunk := response.read(1024 * 1024):
                    digest.update(chunk)
                    output.write(chunk)
        if digest.hexdigest() != asset["sha256"]:
            raise RuntimeError(f"Face model checksum failed: {asset['filename']}")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return path


def _load_models() -> Any:
    global _MODELS
    if _MODELS is None:
        import cv2

        detector_path, recognizer_path = (_model_file(asset) for asset in MODEL_ASSETS)
        detector = cv2.FaceDetectorYN.create(
            str(detector_path), "", (640, 640), DETECTION_CONFIDENCE, 0.3, 5000,
        )
        recognizer = cv2.FaceRecognizerSF.create(str(recognizer_path), "")
        _MODELS = detector, recognizer
    return _MODELS


def warmup_models(model_name: str = MODEL_ID, detector_backend: str = DETECTOR_BACKEND) -> Dict[str, Any]:
    import cv2

    require_model(model_name)
    if detector_backend != DETECTOR_BACKEND:
        raise ValueError("Face ID uses the YuNet detector.")
    _load_models()
    return {
        "ok": True, "warmup": True, "model_id": MODEL_ID, "model_name": MODEL_NAME,
        "detector_backend": DETECTOR_BACKEND, "accelerator": "cpu", "device_name": "CPU",
        "gpu_count": 0, "runtime_version": cv2.__version__,
    }


def _usable_face_area(area: Any) -> bool:
    if not isinstance(area, dict):
        return False
    width, height = max(0, int(area.get("w") or 0)), max(0, int(area.get("h") or 0))
    return min(width, height) >= MIN_FACE_SIDE and width * height >= MIN_FACE_AREA


def _face_crop(image: Any, area: Dict[str, Any]) -> str:
    import cv2

    # Keep context for re-detection on a later upgrade. Recognition itself uses
    # the full-resolution image, independently of the saved gallery thumbnail.
    x, y, width, height = (int(area[key]) for key in ("x", "y", "w", "h"))
    padding = max(4, round(max(width, height) * 0.2))
    crop = image[max(0, y - padding):y + height + padding, max(0, x - padding):x + width + padding]
    if not crop.size:
        return ""
    scale = min(1.0, 320.0 / max(crop.shape[:2]))
    if scale < 1.0:
        crop = cv2.resize(crop, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    ok, encoded = cv2.imencode(".jpg", crop, [int(cv2.IMWRITE_JPEG_QUALITY), 92])
    return base64.b64encode(encoded.tobytes()).decode("ascii") if ok else ""


def represent(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    import cv2
    import numpy as np

    settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
    require_model(settings.get("model_id") or settings.get("model_name"))
    if settings.get("detector_backend", DETECTOR_BACKEND) != DETECTOR_BACKEND:
        raise ValueError("Face ID uses the YuNet detector.")
    raw = base64.b64decode(_text(payload.get("image_b64")), validate=True)
    image = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("Snapshot could not be decoded as an image.")
    detector, recognizer = _load_models()
    maximum = _integer(settings.get("max_faces"), 8, 1, 32)
    minimum_confidence = _number(settings.get("minimum_confidence"), DETECTION_CONFIDENCE, DETECTION_CONFIDENCE, 1.0)
    height, width = image.shape[:2]
    faces = None
    # Retry at a larger size for small faces; map landmarks to the original
    # before alignment so downsampling detection never limits embedding detail.
    for detection_side in (640, 1280):
        scale = min(1.0, detection_side / max(width, height))
        detection_image = cv2.resize(image, (max(1, round(width * scale)), max(1, round(height * scale))))
        detector.setInputSize((detection_image.shape[1], detection_image.shape[0]))
        _, faces = detector.detect(detection_image)
        if faces is not None and len(faces):
            faces = faces.copy()
            faces[:, :14] /= scale
            break
        if scale == 1.0:
            break
    if faces is None:
        return []
    out = []
    for face in sorted(faces, key=lambda row: float(row[14]), reverse=True):
        confidence = float(face[14])
        if confidence < minimum_confidence or not np.isfinite(face).all():
            continue
        x, y = max(0, int(face[0])), max(0, int(face[1]))
        area = {"x": x, "y": y, "w": max(0, min(width - x, int(face[2]))), "h": max(0, min(height - y, int(face[3])))}
        if not _usable_face_area(area):
            continue
        # OpenCV accepts BGR, performs five-landmark alignment, and handles
        # SFace's RGB preprocessing inside feature().
        aligned = recognizer.alignCrop(image, face)
        vector = recognizer.feature(aligned).reshape(-1).astype(np.float32)
        norm = float(np.linalg.norm(vector))
        if len(vector) != EMBEDDING_DIMENSIONS or not np.isfinite(vector).all() or norm <= 0:
            raise RuntimeError("SFace returned an invalid embedding.")
        crop = _face_crop(image, area)
        if not crop:
            continue
        out.append({
            "embedding": (vector / norm).tolist(), "facial_area": area,
            "confidence": confidence, "crop_b64": crop, "crop_content_type": "image/jpeg",
        })
        if len(out) >= maximum:
            break
    return out


def emit(result: Dict[str, Any]) -> None:
    print(RESULT_PREFIX + json.dumps(result, separators=(",", ":")), flush=True)


def handle(payload: Dict[str, Any]) -> Dict[str, Any]:
    request_id = _text(payload.get("request_id"))
    action = _text(payload.get("action")).lower()
    if action == "warmup":
        result = warmup_models(
            _text(payload.get("model_id") or payload.get("model_name")) or MODEL_ID,
            _text(payload.get("detector_backend")) or DETECTOR_BACKEND,
        )
        return {**result, "request_id": request_id}
    if action == "represent":
        return {"ok": True, "request_id": request_id, "detections": represent(payload)}
    if action == "shutdown":
        return {"ok": True, "request_id": request_id, "shutdown": True}
    raise ValueError(f"Unsupported Face ID worker action: {action or 'missing'}")


def warmup(model_name: str, detector_backend: str) -> int:
    try:
        emit(warmup_models(model_name, detector_backend))
        return 0
    except Exception as exc:
        traceback.print_exc()
        emit({"ok": False, "error": _text(exc)})
        return 1


def serve() -> int:
    for line in sys.stdin:
        if not line.strip():
            continue
        request_id = ""
        try:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("Worker payload must be an object.")
            request_id = _text(payload.get("request_id"))
            result = handle(payload)
        except Exception as exc:
            traceback.print_exc()
            result = {"ok": False, "request_id": request_id, "error": _text(exc)}
        emit(result)
        if result.get("shutdown"):
            return 0
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--warmup", nargs=2, metavar=("MODEL", "DETECTOR"))
    args = parser.parse_args()
    if args.warmup:
        return warmup(*args.warmup)
    if args.serve:
        return serve()
    parser.error("choose --serve or --warmup")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
