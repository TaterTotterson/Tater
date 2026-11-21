"""Face geometry, model integrity, and inference contract regression tests."""

import base64
import hashlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import cv2
import numpy as np

from tater.identity import worker


class SFaceWorkerTests(unittest.TestCase):
    def test_maps_five_landmarks_to_original_before_embedding(self):
        image = np.zeros((960, 1280, 3), np.uint8)
        _, encoded = cv2.imencode('.jpg', image)
        # Coordinates in the half-resolution detector input.
        face = np.array([[100, 80, 80, 90, 120, 110, 155, 110, 140, 125, 125, 145, 150, 145, .95]], np.float32)
        detector = Mock()
        detector.detect.return_value = (1, face)
        recognizer = Mock()
        recognizer.feature.return_value = np.ones((1, 128), np.float32)
        with patch.object(worker, '_load_models', return_value=(detector, recognizer)):
            rows = worker.represent({'image_b64': base64.b64encode(encoded).decode()})
        self.assertEqual(len(rows), 1)
        aligned_image, aligned_face = recognizer.alignCrop.call_args.args
        self.assertEqual(aligned_image.shape, image.shape)
        np.testing.assert_allclose(aligned_face[:14], face[0, :14] * 2)
        self.assertEqual(rows[0]['facial_area'], {'x': 200, 'y': 160, 'w': 160, 'h': 180})
        self.assertAlmostEqual(float(np.linalg.norm(rows[0]['embedding'])), 1.0, places=6)

    def test_old_model_request_never_reaches_inference(self):
        with patch.object(worker, '_load_models') as load:
            with self.assertRaises(ValueError):
                worker.represent({'settings': {'model_id': 'facenet512'}})
            load.assert_not_called()

    def test_corrupt_download_does_not_replace_valid_model(self):
        with tempfile.TemporaryDirectory() as folder:
            model = Path(folder) / 'model.onnx'
            model.write_bytes(b'existing artifact')
            asset = {'directory': 'test', 'filename': model.name, 'sha256': hashlib.sha256(b'expected artifact').hexdigest()}
            with (
                patch.dict('os.environ', {'TATER_FACE_MODEL_DIR': folder}),
                patch.object(worker.urllib.request, 'urlopen', return_value=io.BytesIO(b'corrupt download')),
            ):
                with self.assertRaisesRegex(RuntimeError, 'checksum'):
                    worker._model_file(asset)
            self.assertEqual(model.read_bytes(), b'existing artifact')
            self.assertEqual(list(Path(folder).iterdir()), [model])


if __name__ == '__main__':
    unittest.main()
