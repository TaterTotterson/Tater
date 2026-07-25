#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice.wake_verifier import (  # noqa: E402
    build_packet,
    is_wake_verifier_packet,
    parse_packet,
    transcript_match_score,
)


class WakeVerifierPacketTests(unittest.TestCase):
    def test_pcm_packet_round_trip(self) -> None:
        pcm = b"\x01\x02" * 16000
        packet = build_packet(pcm, request_id=42, enforce=True)
        self.assertTrue(is_wake_verifier_packet(packet))
        parsed = parse_packet(packet)
        self.assertEqual(parsed["request_id"], 42)
        self.assertEqual(parsed["sample_count"], 16000)
        self.assertTrue(parsed["enforce"])
        self.assertEqual(parsed["pcm"], pcm)

    def test_packet_size_mismatch_is_rejected(self) -> None:
        packet = build_packet(b"\x00\x00" * 8000, request_id=7)
        with self.assertRaisesRegex(ValueError, "size mismatch"):
            parse_packet(packet[:-2])


class WakeVerifierMatcherTests(unittest.TestCase):
    def test_exact_phrase_inside_short_transcript(self) -> None:
        self.assertEqual(transcript_match_score("Okay. Hey Tater!", "hey_tater"), 1.0)

    def test_known_stt_variant_clears_default_threshold(self) -> None:
        self.assertGreaterEqual(transcript_match_score("Hey, Dater.", "Hey Tater"), 0.85)

    def test_known_false_variant_stays_below_default_threshold(self) -> None:
        self.assertLess(transcript_match_score("Hey, Tanner.", "Hey Tater"), 0.85)

    def test_unrelated_speech_does_not_match(self) -> None:
        self.assertLess(transcript_match_score("That's cool, thank you.", "Hey Tater"), 0.5)


if __name__ == "__main__":
    unittest.main()
