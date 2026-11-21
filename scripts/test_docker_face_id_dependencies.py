"""Run the Docker dependency gates with the current Face ID package contract."""

import contextlib
import importlib.util
import io
from pathlib import Path
import re
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
RETIRED_MODULES = {"deepface", "retinaface", "tensorflow", "tf_keras", "omegaconf", "fvcore"}


def python_checks(filename):
    return re.findall(r'python -c "([^"]+)"', (ROOT / filename).read_text())


class DockerFaceIdDependenciesTests(unittest.TestCase):
    def test_dependency_gate_accepts_retired_packages_being_absent(self):
        for filename in ("Dockerfile", "Dockerfile.nvidia"):
            checks = [code for code in python_checks(filename) if "find_spec" in code]
            self.assertEqual(len(checks), 1, filename)
            with self.subTest(filename=filename), patch.object(
                importlib.util, "find_spec",
                side_effect=lambda name: None if name in RETIRED_MODULES else object(),
            ):
                exec(checks[0], {})

    def test_dependency_gate_still_rejects_missing_opencv_and_numpy(self):
        for filename in ("Dockerfile", "Dockerfile.nvidia"):
            code = next(code for code in python_checks(filename) if "find_spec" in code)
            for missing in ("cv2", "numpy"):
                with self.subTest(filename=filename, missing=missing), patch.object(
                    importlib.util, "find_spec",
                    side_effect=lambda name: None if name == missing else object(),
                ):
                    with self.assertRaises(AssertionError):
                        exec(code, {})

    @unittest.skipUnless(importlib.util.find_spec("cv2"), "OpenCV APIs are checked inside each image")
    def test_both_images_import_the_actual_yunet_and_sface_apis(self):
        for filename in ("Dockerfile", "Dockerfile.nvidia"):
            checks = [code for code in python_checks(filename) if "FaceDetectorYN" in code]
            self.assertEqual(len(checks), 1, filename)
            with self.subTest(filename=filename), contextlib.redirect_stdout(io.StringIO()):
                exec(checks[0], {})


if __name__ == "__main__":
    unittest.main()
