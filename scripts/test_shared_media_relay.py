from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import shared_media_relay


class _FakeResponse:
    def __init__(self, chunks, *, release_event: threading.Event | None = None):
        self.headers = {"Content-Type": "audio/mpeg"}
        self._chunks = list(chunks)
        self._release_event = release_event
        self.closed = False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        del chunk_size
        for index, chunk in enumerate(self._chunks):
            if index and self._release_event is not None:
                self._release_event.wait(timeout=2.0)
            yield chunk

    def close(self):
        self.closed = True


class SharedMediaRelayTests(unittest.TestCase):
    def tearDown(self) -> None:
        shared_media_relay.shutdown_shared_media_relays()

    def test_two_readers_receive_identical_bytes_from_one_upstream_request(self) -> None:
        release = threading.Event()
        first_chunk = b"a" * shared_media_relay.RELAY_READY_BYTES
        second_chunk = b"b" * 8192
        response = _FakeResponse([first_chunk, second_chunk], release_event=release)

        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            shared_media_relay,
            "_runtime_root",
            return_value=Path(temp_dir),
        ), mock.patch.object(
            shared_media_relay.requests,
            "get",
            return_value=response,
        ) as get:
            relay = shared_media_relay.register_shared_media_relay(
                "http://music.test/live.mp3",
                media_type="audio/mpeg",
                filename="live.mp3",
            )
            first, first_type, _first_name = shared_media_relay.open_shared_media_relay(
                relay["relay_id"], relay["token"]
            )
            second, second_type, _second_name = shared_media_relay.open_shared_media_relay(
                relay["relay_id"], relay["token"]
            )

            self.assertEqual(next(first), first_chunk)
            self.assertEqual(next(second), first_chunk)
            release.set()
            self.assertEqual(b"".join(first), second_chunk)
            self.assertEqual(b"".join(second), second_chunk)

        self.assertEqual(get.call_count, 1)
        self.assertEqual(first_type, "audio/mpeg")
        self.assertEqual(second_type, "audio/mpeg")

    def test_invalid_token_does_not_open_relay(self) -> None:
        response = _FakeResponse([b"x" * shared_media_relay.RELAY_READY_BYTES])
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            shared_media_relay,
            "_runtime_root",
            return_value=Path(temp_dir),
        ), mock.patch.object(
            shared_media_relay.requests,
            "get",
            return_value=response,
        ):
            relay = shared_media_relay.register_shared_media_relay(
                "http://music.test/live.mp3"
            )
            with self.assertRaises(shared_media_relay.SharedMediaRelayError):
                shared_media_relay.open_shared_media_relay(
                    relay["relay_id"], "wrong-token"
                )

    def test_finished_mp3_is_shared_and_recovery_starts_on_a_frame(self) -> None:
        # MPEG-1 Layer III, 192 kbps, 44.1 kHz: 626 bytes per frame.
        frames = [b"\xff\xfb\xb0\x00" + bytes([index]) * 622 for index in range(100)]
        mp3 = b"".join(frames)
        response = _FakeResponse([mp3])
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            shared_media_relay,
            "_runtime_root",
            return_value=Path(temp_dir),
        ), mock.patch.object(
            shared_media_relay.requests,
            "get",
            return_value=response,
        ) as get:
            relay = shared_media_relay.register_shared_media_relay(
                "http://music.test/track.mp3",
                media_type="audio/mpeg",
                filename="track.mp3",
            )
            shared_media_relay._authorized_relay(relay["relay_id"], relay["token"]).thread.join(timeout=2.0)
            info = shared_media_relay.describe_shared_media_relay(
                relay["relay_id"], relay["token"]
            )
            self.assertTrue(info["complete"])
            self.assertEqual(info["bytes_written"], len(mp3))
            self.assertEqual(info["path"].read_bytes(), mp3)
            recovered, media_type, _ = shared_media_relay.open_shared_media_relay(
                relay["relay_id"], relay["token"], start_seconds=0.5
            )
            self.assertEqual(b"".join(recovered), b"".join(frames[20:]))
            self.assertEqual(media_type, "audio/mpeg")
            self.assertEqual(get.call_count, 1)

    def test_finite_source_waits_for_completion_before_dispatch(self) -> None:
        response = _FakeResponse([b"x" * shared_media_relay.RELAY_READY_BYTES])
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            shared_media_relay,
            "_runtime_root",
            return_value=Path(temp_dir),
        ), mock.patch.object(
            shared_media_relay.requests,
            "get",
            return_value=response,
        ):
            relay = shared_media_relay.register_shared_media_relay(
                "http://music.test/track.mp3", completion_wait_s=2.0
            )
            self.assertTrue(relay["complete"])
            self.assertEqual(relay["initial_bytes"], shared_media_relay.RELAY_READY_BYTES)

    def test_finite_track_remains_available_for_its_expected_playback(self) -> None:
        response = _FakeResponse([b"x" * shared_media_relay.RELAY_READY_BYTES])
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            shared_media_relay,
            "_runtime_root",
            return_value=Path(temp_dir),
        ), mock.patch.object(
            shared_media_relay.requests,
            "get",
            return_value=response,
        ):
            relay = shared_media_relay.register_shared_media_relay(
                "http://music.test/long-track.mp3",
                completion_wait_s=2.0,
                expected_duration_seconds=3600.0,
            )
            relay_row = shared_media_relay._authorized_relay(
                relay["relay_id"], relay["token"]
            )
            self.assertEqual(relay_row.retention_seconds, 4200.0)
            with shared_media_relay._registry_lock:
                shared_media_relay._prune_relays_locked(
                    now=relay_row.completed_at + 11 * 60
                )
                self.assertIn(relay["relay_id"], shared_media_relay._relays)
                shared_media_relay._prune_relays_locked(
                    now=relay_row.completed_at + 4201
                )
                self.assertNotIn(relay["relay_id"], shared_media_relay._relays)

    def test_live_mp3_recovery_reads_from_the_current_shared_spool(self) -> None:
        frames = [b"\xff\xfb\xb0\x00" + bytes([index]) * 622 for index in range(60)]
        release = threading.Event()
        response = _FakeResponse(
            [b"".join(frames[:40]), b"".join(frames[40:])],
            release_event=release,
        )
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            shared_media_relay,
            "_runtime_root",
            return_value=Path(temp_dir),
        ), mock.patch.object(
            shared_media_relay.requests,
            "get",
            return_value=response,
        ) as get:
            relay = shared_media_relay.register_shared_media_relay(
                "http://airplay.test/live.mp3",
                media_type="audio/mpeg",
                filename="live.mp3",
            )
            self.assertFalse(relay["complete"])
            recovered, _, _ = shared_media_relay.open_shared_media_relay(
                relay["relay_id"], relay["token"], start_seconds=0.5
            )
            self.assertEqual(next(recovered) + next(recovered), b"".join(frames[20:40]))
            release.set()
            self.assertEqual(b"".join(recovered), b"".join(frames[40:]))
            self.assertEqual(get.call_args.kwargs["stream"], True)
            self.assertLessEqual(shared_media_relay.RELAY_IO_CHUNK_BYTES, 8 * 1024)

    def test_recovery_does_not_silently_restart_non_mp3_audio(self) -> None:
        response = _FakeResponse([b"x" * shared_media_relay.RELAY_READY_BYTES])
        response.headers["Content-Type"] = "audio/wav"
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            shared_media_relay,
            "_runtime_root",
            return_value=Path(temp_dir),
        ), mock.patch.object(
            shared_media_relay.requests,
            "get",
            return_value=response,
        ):
            relay = shared_media_relay.register_shared_media_relay(
                "http://music.test/track.wav", media_type="audio/wav"
            )
            with self.assertRaises(shared_media_relay.SharedMediaRelayError):
                shared_media_relay.open_shared_media_relay(
                    relay["relay_id"], relay["token"], start_seconds=1.0
                )


if __name__ == "__main__":
    unittest.main()
