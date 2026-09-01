"""Isolated DeepFace worker used by Tater's optional Face ID runtime."""

from __future__ import annotations

import argparse
import base64
import contextlib
import json
import os
import platform
import sys
import sysconfig
import traceback
from pathlib import Path
from typing import Any, Dict, List


os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
# RetinaFace still builds its detector with the legacy Keras API. This must be
# selected before TensorFlow is imported or Keras 3 rejects its symbolic graph.
os.environ["TF_USE_LEGACY_KERAS"] = "1"
RESULT_PREFIX = "TATER_FACE_RESULT:"
_DEEPFACE: Any = None
_ADAFACE_MODELS: Dict[str, Any] = {}
_DEVICE_INFO: Dict[str, Any] = {}

FACENET_MODEL_ID = "facenet512"
ADAFACE_MODEL_ID = "adaface_ir50_webface4m"


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


def _model_id(value: Any) -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"adaface", "adaface_ir50", ADAFACE_MODEL_ID}:
        return ADAFACE_MODEL_ID
    return FACENET_MODEL_ID


def _adaface_model_dir() -> Path:
    root = Path(_text(os.environ.get("TATER_FACE_MODEL_DIR")) or ".")
    return (root / "adaface-ir50-webface4m").resolve()


def _load_adaface() -> Any:
    model_id = ADAFACE_MODEL_ID
    if model_id in _ADAFACE_MODELS:
        return _ADAFACE_MODELS[model_id]

    from huggingface_hub import snapshot_download

    repo_id = _text(os.environ.get("TATER_ADAFACE_REPO_ID")) or "minchul/cvlface_adaface_ir50_webface4m"
    revision = _text(os.environ.get("TATER_ADAFACE_REVISION")) or None
    model_dir = _adaface_model_dir()
    model_dir.mkdir(parents=True, exist_ok=True)
    required = (
        model_dir / "wrapper.py",
        model_dir / "models" / "__init__.py",
        model_dir / "models" / "iresnet" / "model.py",
        model_dir / "pretrained_model" / "model.yaml",
        model_dir / "pretrained_model" / "model.pt",
    )
    if not all(path.is_file() for path in required):
        snapshot_download(
            repo_id=repo_id,
            revision=revision,
            local_dir=str(model_dir),
            allow_patterns=[
                "config.json",
                "wrapper.py",
                "models/*.py",
                "models/**/*.py",
                "models/**/*.yaml",
                "pretrained_model/*.yaml",
                "pretrained_model/model.pt",
            ],
        )

    import torch

    old_cwd = Path.cwd()
    sys.path.insert(0, str(model_dir))
    try:
        os.chdir(model_dir)
        from wrapper import CVLFaceRecognitionModel, ModelConfig

        model = CVLFaceRecognitionModel(ModelConfig())
    finally:
        os.chdir(old_cwd)
        with contextlib.suppress(ValueError):
            sys.path.remove(str(model_dir))

    if torch.cuda.is_available():
        device = torch.device("cuda")
    elif bool(getattr(torch.backends, "mps", None)) and torch.backends.mps.is_available():
        device = torch.device("mps")
    else:
        device = torch.device("cpu")
    model = model.to(device)
    model.eval()
    _ADAFACE_MODELS[model_id] = (model, device)
    return _ADAFACE_MODELS[model_id]


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
    model_id = _model_id(model_name)
    if model_id == ADAFACE_MODEL_ID:
        _load_adaface()
    else:
        deepface.build_model("Facenet512")
    deepface.build_model(detector_backend or "retinaface", task="face_detector")
    return {
        "ok": True,
        "warmup": True,
        "model_id": model_id,
        "model_name": "AdaFace IR-50 WebFace4M" if model_id == ADAFACE_MODEL_ID else "Facenet512",
        "detector_backend": detector_backend or "retinaface",
        **_DEVICE_INFO,
    }


def _adaface_represent(
    deepface: Any,
    image: Any,
    *,
    detector_backend: str,
    maximum: int,
    minimum_confidence: float,
) -> List[Dict[str, Any]]:
    import cv2
    import numpy as np
    import torch

    faces = deepface.extract_faces(
        img_path=image,
        detector_backend=detector_backend,
        enforce_detection=True,
        align=True,
        color_face="rgb",
        normalize_face=True,
    )
    rows = faces if isinstance(faces, list) else [faces]
    accepted = [
        row
        for row in rows[:maximum]
        if isinstance(row, dict)
        and isinstance(row.get("face"), np.ndarray)
        and _number(row.get("confidence"), 1.0, 0.0, 1.0) >= minimum_confidence
    ]
    if not accepted:
        return []

    batch: List[Any] = []
    for row in accepted:
        face = row["face"]
        if face.shape[:2] != (112, 112):
            face = cv2.resize(face, (112, 112), interpolation=cv2.INTER_AREA)
        tensor = torch.from_numpy(np.ascontiguousarray(face.transpose(2, 0, 1))).float()
        batch.append((tensor - 0.5) / 0.5)

    model, device = _load_adaface()
    input_batch = torch.stack(batch, dim=0).to(device)
    with torch.inference_mode():
        features = model(input_batch)
        if isinstance(features, (tuple, list)):
            features = features[0]
        elif isinstance(features, dict):
            output = features
            features = output.get("embeddings")
            if features is None:
                features = output.get("last_hidden_state")
        if features is None:
            raise RuntimeError("AdaFace did not return an embedding tensor.")
        features = torch.nn.functional.normalize(features.float(), p=2, dim=1)
        vectors = features.detach().cpu().tolist()

    out: List[Dict[str, Any]] = []
    for row, embedding in zip(accepted, vectors):
        area = row.get("facial_area") if isinstance(row.get("facial_area"), dict) else {}
        out.append(
            {
                "embedding": [float(value) for value in embedding],
                "facial_area": {
                    "x": int(area.get("x") or 0),
                    "y": int(area.get("y") or 0),
                    "w": int(area.get("w") or area.get("width") or 0),
                    "h": int(area.get("h") or area.get("height") or 0),
                },
                "confidence": _number(row.get("confidence"), 1.0, 0.0, 1.0),
                "crop_b64": _face_crop(image, area),
                "crop_content_type": "image/jpeg",
            }
        )
    return out


def represent(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    import cv2
    import numpy as np

    deepface = _load_deepface()
    settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
    model_id = _model_id(settings.get("model_id") or settings.get("model_name"))
    raw_image = base64.b64decode(_text(payload.get("image_b64")), validate=True)
    image = cv2.imdecode(np.frombuffer(raw_image, dtype=np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("Snapshot could not be decoded as an image.")
    maximum = _integer(settings.get("max_faces"), 8, 1, 32)
    minimum_confidence = _number(settings.get("minimum_confidence"), 0.0, 0.0, 1.0)
    try:
        if model_id == ADAFACE_MODEL_ID:
            return _adaface_represent(
                deepface,
                image,
                detector_backend=_text(settings.get("detector_backend")) or "retinaface",
                maximum=maximum,
                minimum_confidence=minimum_confidence,
            )
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
            _text(payload.get("model_id") or payload.get("model_name")) or FACENET_MODEL_ID,
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
