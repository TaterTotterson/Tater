#!/usr/bin/env python3
"""Compare Tater Face ID models on a labeled folder of face images.

Dataset layout::

    faces/
      Fred/
        front.jpg
        porch.jpg
      Wilma/
        front.jpg
        kitchen.jpg

The tool is read-only with respect to Tater's Redis identity store. It reports
same-person and different-person cosine distances plus a data-derived threshold
for each model.
"""

from __future__ import annotations

import argparse
import base64
import itertools
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tater_paths import runtime_dir

os.environ.setdefault("TATER_FACE_MODEL_DIR", str(runtime_dir() / "models" / "face-id"))

import face_id_runtime
import face_id_worker
from face_identity import cosine_distance


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    position = (len(ordered) - 1) * max(0.0, min(1.0, percentile))
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def _summary(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "minimum": round(min(values), 6),
        "median": round(statistics.median(values), 6),
        "p95": round(_percentile(values, 0.95), 6),
        "maximum": round(max(values), 6),
    }


def _balanced_threshold(genuine: List[float], impostor: List[float]) -> Dict[str, Any]:
    if not genuine or not impostor:
        return {}
    ordered = sorted(set([*genuine, *impostor]))
    candidates = [ordered[0] - 1e-6, ordered[-1] + 1e-6]
    candidates.extend((left + right) / 2.0 for left, right in zip(ordered, ordered[1:]))
    best: Tuple[float, float, float, float] | None = None
    for threshold in candidates:
        true_accept = sum(distance <= threshold for distance in genuine) / len(genuine)
        true_reject = sum(distance > threshold for distance in impostor) / len(impostor)
        balanced = (true_accept + true_reject) / 2.0
        candidate = (balanced, true_reject, true_accept, -threshold)
        if best is None or candidate > best:
            best = candidate
    assert best is not None
    result = {
        "threshold": round(-best[3], 6),
        "balanced_accuracy": round(best[0], 6),
        "true_reject_rate": round(best[1], 6),
        "true_accept_rate": round(best[2], 6),
    }
    result["enough_data_to_consider"] = len(genuine) >= 5 and len(impostor) >= 10
    if not result["enough_data_to_consider"]:
        result["warning"] = "Collect at least 5 same-person and 10 different-person pairs before changing Tater's configured threshold."
    return result


def _configured_metrics(genuine: List[float], impostor: List[float], threshold: float) -> Dict[str, Any]:
    if not genuine or not impostor:
        return {}
    true_accept = sum(distance <= threshold for distance in genuine) / len(genuine)
    true_reject = sum(distance > threshold for distance in impostor) / len(impostor)
    return {
        "balanced_accuracy": round((true_accept + true_reject) / 2.0, 6),
        "true_accept_rate": round(true_accept, 6),
        "true_reject_rate": round(true_reject, 6),
    }


def discover_images(dataset: Path) -> List[Tuple[str, Path]]:
    rows: List[Tuple[str, Path]] = []
    for person_dir in sorted(path for path in dataset.iterdir() if path.is_dir()):
        for image_path in sorted(person_dir.rglob("*")):
            if image_path.is_file() and image_path.suffix.lower() in IMAGE_SUFFIXES:
                rows.append((person_dir.name, image_path))
    return rows


def embed_image(path: Path, model_id: str) -> List[float]:
    detections = face_id_worker.represent(
        {
            "image_b64": base64.b64encode(path.read_bytes()).decode("ascii"),
            "settings": {
                "model_id": model_id,
                "detector_backend": face_id_runtime.DETECTOR_BACKEND,
                "minimum_confidence": 0.0,
                "max_faces": 2,
            },
        }
    )
    if len(detections) != 1:
        raise ValueError(f"expected exactly one face, found {len(detections)}")
    embedding = detections[0].get("embedding")
    if not isinstance(embedding, list) or not embedding:
        raise ValueError("model returned no embedding")
    return [float(value) for value in embedding]


def evaluate_model(images: Iterable[Tuple[str, Path]], model_id: str) -> Dict[str, Any]:
    model = face_id_runtime.selected_model(model_id=model_id)
    started = time.monotonic()
    embedded: List[Tuple[str, Path, List[float]]] = []
    failures: List[Dict[str, str]] = []
    for label, image_path in images:
        try:
            embedded.append((label, image_path, embed_image(image_path, model["id"])))
        except Exception as exc:
            failures.append({"label": label, "image": str(image_path), "error": str(exc)})

    genuine: List[float] = []
    impostor: List[float] = []
    for left, right in itertools.combinations(embedded, 2):
        distance = cosine_distance(left[2], right[2])
        (genuine if left[0] == right[0] else impostor).append(distance)

    configured_threshold = float(model["match_threshold"])
    return {
        "model_id": model["id"],
        "model": model["label"],
        "configured_threshold": configured_threshold,
        "images_embedded": len(embedded),
        "failures": failures,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "genuine_distances": _summary(genuine),
        "impostor_distances": _summary(impostor),
        "configured_threshold_results": _configured_metrics(genuine, impostor, configured_threshold),
        "data_derived_threshold": _balanced_threshold(genuine, impostor),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path, help="Folder containing one subfolder per person")
    parser.add_argument(
        "--models",
        default="facenet512,adaface_ir50_webface4m",
        help="Comma-separated Face ID model IDs",
    )
    parser.add_argument("--json-output", type=Path, help="Optional path for the full JSON report")
    args = parser.parse_args()

    dataset = args.dataset.expanduser().resolve()
    if not dataset.is_dir():
        parser.error(f"dataset folder does not exist: {dataset}")
    images = discover_images(dataset)
    labels = sorted({label for label, _path in images})
    if len(images) < 2:
        parser.error("add at least two images under person subfolders")
    if len(labels) < 2:
        parser.error("add at least two different person subfolders to measure false matches")

    model_ids = [face_id_runtime.normalize_model_id(value) for value in args.models.split(",") if value.strip()]
    report = {
        "dataset": str(dataset),
        "people": len(labels),
        "images": len(images),
        "models": [evaluate_model(images, model_id) for model_id in dict.fromkeys(model_ids)],
    }
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.json_output:
        args.json_output.expanduser().resolve().write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
