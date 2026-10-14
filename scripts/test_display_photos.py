#!/usr/bin/env python3
from __future__ import annotations

import io
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

from PIL import Image

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import display_photos  # noqa: E402


def _png(width: int = 2400, height: int = 1400) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (width, height), (31, 93, 146)).save(output, format="PNG")
    return output.getvalue()


class DisplayPhotosTests(unittest.TestCase):
    def test_uploads_are_normalized_isolated_and_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ,
            {"TATER_DISPLAY_PHOTO_DIR": directory},
        ):
            first = display_photos.save_photo(
                "native:checkers-a",
                _png(),
                filename="lake.png",
                content_type="image/png",
            )
            display_photos.save_photo(
                "native:crown-b",
                _png(640, 480),
                filename="forest.png",
                content_type="image/png",
            )

            rows = display_photos.list_photos("native:checkers-a")
            self.assertEqual([row["id"] for row in rows], [first["id"]])
            self.assertEqual(display_photos.resolve_photo(first["id"]).suffix, ".jpg")
            with Image.open(display_photos.resolve_photo(first["id"])) as normalized:
                self.assertLessEqual(normalized.width, display_photos.MAX_SIZE[0])
                self.assertLessEqual(normalized.height, display_photos.MAX_SIZE[1])

            urls = display_photos.firmware_urls("native:checkers-a", "http://10.0.0.5:8123/")
            self.assertEqual(len(urls), 1)
            self.assertTrue(urls[0].startswith("http://10.0.0.5:8123/api/tater/satellite/v1/display-photo/"))

            with self.assertRaises(ValueError):
                display_photos.remove_photo("native:crown-b", first["id"])
            self.assertTrue(display_photos.remove_photo("native:checkers-a", first["id"]))
            self.assertEqual(display_photos.list_photos("native:checkers-a"), [])

    def test_rejects_unsupported_or_oversized_uploads(self) -> None:
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ,
            {"TATER_DISPLAY_PHOTO_DIR": directory},
        ):
            with self.assertRaisesRegex(ValueError, "JPEG, PNG, or WebP"):
                display_photos.save_photo("native:checkers-a", b"not-heic", content_type="image/heic")
            with self.assertRaisesRegex(ValueError, "12 MB"):
                display_photos.save_photo(
                    "native:checkers-a",
                    b"x" * (display_photos.MAX_UPLOAD_BYTES + 1),
                    content_type="image/jpeg",
                )


if __name__ == "__main__":
    unittest.main()
