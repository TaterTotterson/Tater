"""Isolated DeepFace worker used by Tater's optional Face ID runtime."""

from __future__ import annotations

import argparse
import base64
import json
import os
import platform
import sys
import sysconfig
import traceback
from typing import Any, Dict, List


os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
# RetinaFace still builds its detector with the legacy Keras API. This must be
# selected before TensorFlow is imported or Keras 3 rejects its symbolic graph.
os.environ["TF_USE_LEGACY_KERAS"] = "1"
RESULT_PREFIX = "TATER_FACE_RESULT:"
_DEEPFACE: Any = None
_DEVICE_INFO: Dict[str, Any] = {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _load_deepface() -> Any:
    global _DEEPFACE, _DEVICE_INFO
    if _DEEPFACE is not None:
        return _DEEPFACE
    original_get_scheme_names = sysconfig.get_scheme_names
    allowed = {"venv", "posix_venv", "posix_prefix", "nt_venv"}
    sysconfig.get_scheme_names = lambda: tuple(
        name for name in original_get_scheme_names() if name in allowed
    )
    try:
        import tensorflow as tf

        tf.config.set_soft_device_placement(True)
        physical_gpus = list(tf.config.list_physical_devices("GPU"))
        for device in physical_gpus:
            try:
                tf.config.experimental.set_memory_growth(device, True)
            except Exception:
                pass
        device_names: List[str] = []
        for device in physical_gpus:
            name = _text(getattr(device, "name", ""))
            try:
                details = tf.config.experimental.get_device_details(device)
                name = _text(details.get("device_name")) or name
            except Exception:
                pass
            if name:
                device_names.append(name)
        if physical_gpus:
            accelerator = "metal" if sys.platform == "darwin" else "cuda"
        else:
            accelerator = "cpu"
        _DEVICE_INFO = {
            "accelerator": accelerator,
            "device_name": ", ".join(device_names) or ("CPU" if not physical_gpus else "GPU"),
            "gpu_count": len(physical_gpus),
            "tensorflow_version": _text(getattr(tf, "__version__", "")),
            "platform": sys.platform,
            "machine": platform.machine(),
        }
        from deepface import DeepFace

        _DEEPFACE = DeepFace
        return DeepFace
    finally:
        sysconfig.get_scheme_names = original_get_scheme_names


def _number(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    return max(float(minimum), min(float(maximum), parsed))


def _integer(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _face_crop(image: Any, area: Dict[str, Any]) -> str:
    import cv2

    x = max(0, int(area.get("x") or 0))
    y = max(0, int(area.get("y") or 0))
    width = max(1, int(area.get("w") or area.get("width") or 1))
    height = max(1, int(area.get("h") or area.get("height") or 1))
    crop = image[y : y + height, x : x + width]
    if getattr(crop, "size", 0) <= 0:
        return ""
    max_side = max(int(crop.shape[0]), int(crop.shape[1]))
    if max_side > 240:
        scale = 240.0 / max_side
        crop = cv2.resize(crop, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    ok, encoded = cv2.imencode(".jpg", crop, [int(cv2.IMWRITE_JPEG_QUALITY), 86])
    if not ok:
        return ""
    return base64.b64encode(encoded.tobytes()).decode("ascii")


def warmup_models(model_name: str, detector_backend: str) -> Dict[str, Any]:
    deepface = _load_deepface()
    deepface.build_model(model_name or "Facenet512")
    deepface.build_model(detector_backend or "retinaface", task="face_detector")
    return {
        "ok": True,
        "warmup": True,
        "model_name": model_name or "Facenet512",
        "detector_backend": detector_backend or "retinaface",
        **_DEVICE_INFO,
    }


def represent(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    import cv2
    import numpy as np

    deepface = _load_deepface()
    settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
    raw_image = base64.b64decode(_text(payload.get("image_b64")), validate=True)
    image = cv2.imdecode(np.frombuffer(raw_image, dtype=np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("Snapshot could not be decoded as an image.")
    try:
        represented = deepface.represent(
            img_path=image,
            model_name=_text(settings.get("model_name")) or "Facenet512",
            detector_backend=_text(settings.get("detector_backend")) or "retinaface",
            enforce_detection=True,
            align=True,
        )
    except ValueError as exc:
        message = _text(exc).lower()
        if "face could not" in message or "no face" in message or ("face" in message and "detect" in message):
            return []
        raise

    rows = represented if isinstance(represented, list) else [represented]
    maximum = _integer(settings.get("max_faces"), 8, 1, 32)
    minimum_confidence = _number(settings.get("minimum_confidence"), 0.0, 0.0, 1.0)
    out: List[Dict[str, Any]] = []
    for row in rows[:maximum]:
        if not isinstance(row, dict) or not isinstance(row.get("embedding"), list):
            continue
        confidence = _number(row.get("face_confidence"), 1.0, 0.0, 1.0)
        if confidence < minimum_confidence:
            continue
        area = row.get("facial_area") if isinstance(row.get("facial_area"), dict) else {}
        out.append(
            {
                "embedding": [float(item) for item in row["embedding"]],
                "facial_area": {
                    "x": int(area.get("x") or 0),
                    "y": int(area.get("y") or 0),
                    "w": int(area.get("w") or area.get("width") or 0),
                    "h": int(area.get("h") or area.get("height") or 0),
                },
                "confidence": confidence,
                "crop_b64": _face_crop(image, area),
                "crop_content_type": "image/jpeg",
            }
        )
    return out


def emit(result: Dict[str, Any]) -> None:
    print(RESULT_PREFIX + json.dumps(result, separators=(",", ":")), flush=True)


def handle(payload: Dict[str, Any]) -> Dict[str, Any]:
    request_id = _text(payload.get("request_id"))
    action = _text(payload.get("action")).lower()
    if action == "warmup":
        result = warmup_models(
            _text(payload.get("model_name")) or "Facenet512",
            _text(payload.get("detector_backend")) or "retinaface",
        )
        result["request_id"] = request_id
        return result
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
        if bool(result.get("shutdown")):
            return 0
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--warmup", nargs=2, metavar=("MODEL", "DETECTOR"))
    args = parser.parse_args()
    if args.warmup:
        return warmup(args.warmup[0], args.warmup[1])
    if args.serve:
        return serve()
    parser.error("choose --serve or --warmup")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
