"""The single supported Face ID model and its pinned upstream artifacts."""

MODEL_ID = "sface"
MODEL_NAME = "SFace"
DETECTOR_BACKEND = "yunet"
EMBEDDING_DIMENSIONS = 128
# OpenCV's SFace reference accepts cosine similarity >= 0.363. Tater uses
# cosine distance, 1 - similarity. Keep unknown clustering more conservative.
MATCH_THRESHOLD = 0.637
ANONYMOUS_MATCH_THRESHOLD = 0.45
MATCH_MIN_MARGIN = 0.08
MODEL_REVISION = "48b10fa505aeb51fbd127fc60a5c660040e823b7"
MODEL_ASSETS = (
    {
        "directory": "face_detection_yunet",
        "filename": "face_detection_yunet_2026may.onnx",
        "sha256": "ebafce4e3c118d6554634be5c27ab333b4c047a9a8c3faf1d7cf93101c22f0f0",
    },
    {
        "directory": "face_recognition_sface",
        "filename": "face_recognition_sface_2021dec.onnx",
        "sha256": "0ba9fbfa01b5270c96627c4ef784da859931e02f04419c829e83484087c34e79",
    },
)


def require_model(value: object = "") -> str:
    """Refuse stale clients rather than labeling another model's vectors SFace."""
    token = str(value or "").strip().lower()
    if token not in {"", MODEL_ID}:
        raise ValueError("Face ID uses YuNet + SFace. Update this Tater and its Spud Hub to the same version.")
    return MODEL_ID
