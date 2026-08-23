import json
import unittest
from pathlib import Path
from unittest.mock import patch

import face_identity
import people


ROOT = Path(__file__).resolve().parents[1]


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.lists = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hset(self, key, field=None, value=None, mapping=None):
        target = self.hashes.setdefault(key, {})
        if mapping:
            target.update(mapping)
        elif field is not None:
            target[field] = value
        return 1

    def hdel(self, key, field):
        target = self.hashes.get(key, {})
        existed = field in target
        target.pop(field, None)
        return int(existed)

    def lrange(self, key, start, end):
        rows = list(self.lists.get(key) or [])
        return rows[start:] if end < 0 else rows[start : end + 1]


class SharedFaceIdentityTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()

    @staticmethod
    def detection(embedding):
        return {
            "embedding": embedding,
            "facial_area": {"x": 1, "y": 2, "w": 120, "h": 120},
            "confidence": 0.98,
            "crop_b64": "ZmFjZQ==",
            "crop_content_type": "image/jpeg",
        }

    def test_shared_store_starts_clean_without_legacy_awareness_fallback(self):
        self.redis.hashes["awareness:face_identities"] = {
            "face_old": json.dumps({"id": "face_old", "name": "Old Face"})
        }

        self.assertEqual(face_identity.identity_rows(self.redis), {})

    def test_recording_matches_faces_and_deduplicates_the_same_event(self):
        first = face_identity.record_detection(
            self.detection([1.0, 0.0]),
            event_id="event-one",
            seen_at="2026-08-23T10:00:00Z",
            redis_client=self.redis,
        )
        duplicate = face_identity.record_detection(
            self.detection([0.999, 0.001]),
            event_id="event-one",
            seen_at="2026-08-23T10:00:01Z",
            redis_client=self.redis,
        )
        later = face_identity.record_detection(
            self.detection([0.998, 0.002]),
            event_id="event-two",
            seen_at="2026-08-23T10:05:00Z",
            redis_client=self.redis,
        )

        self.assertEqual(duplicate["id"], first["id"])
        self.assertEqual(later["id"], first["id"])
        saved = face_identity.identity_rows(self.redis)[first["id"]]
        self.assertEqual(saved["observation_count"], 2)
        self.assertEqual(saved["event_count"], 2)

    def test_people_face_action_links_the_shared_profile(self):
        person = people.create_person("Fred", self.redis)
        identity = face_identity.record_detection(
            self.detection([1.0, 0.0]),
            event_id="front-door",
            redis_client=self.redis,
        )

        result = people.handle_action(
            "people_face_save",
            {
                "identity_id": identity["id"],
                "values": {"person_id": person["id"], "name": ""},
            },
            self.redis,
        )

        face = result["people"]["faces"][0]
        self.assertTrue(face["linked"])
        self.assertEqual(face["person_name"], "Fred")
        self.assertEqual(face["gallery"][0]["event_id"], "front-door")
        self.assertEqual(
            face_identity.recognized_people([identity["id"]], self.redis)[0]["person_name"],
            "Fred",
        )

    def test_moving_images_keeps_event_identity_resolution_current(self):
        source = face_identity.record_detection(
            self.detection([1.0, 0.0]),
            event_id="event-source",
            redis_client=self.redis,
        )
        target = face_identity.record_detection(
            self.detection([0.0, 1.0]),
            event_id="event-target",
            redis_client=self.redis,
        )
        observation_id = face_identity.observations(source)[0]["id"]

        result = face_identity.move_observations(
            source["id"],
            [observation_id],
            target_id=target["id"],
            redis_client=self.redis,
        )

        self.assertTrue(result["source_removed"])
        self.assertEqual(
            face_identity.identity_ids_for_event("event-source", [source["id"]], self.redis),
            [target["id"]],
        )

    def test_removing_an_events_last_face_overrides_the_old_session_reference(self):
        identity = face_identity.record_detection(
            self.detection([1.0, 0.0]),
            event_id="event-remove",
            redis_client=self.redis,
        )
        observation_id = face_identity.observations(identity)[0]["id"]

        face_identity.remove_observations(
            identity["id"],
            [observation_id],
            redis_client=self.redis,
        )

        self.assertEqual(
            face_identity.identity_ids_for_event("event-remove", [identity["id"]], self.redis),
            [],
        )

    def test_automation_style_recognition_records_unknown_faces(self):
        with (
            patch.object(face_identity, "runtime_status", return_value={"enabled": True, "loaded": True}),
            patch.object(face_identity.face_id_runtime, "analyze_image", return_value=[self.detection([1.0, 0.0])]),
        ):
            result = face_identity.recognize_image(
                b"jpeg",
                event_id="automation-doorbell",
                source={"owner": "automation"},
                redis_client=self.redis,
            )

        self.assertEqual(result["status"], "unrecognized")
        self.assertEqual(result["faces_detected"], 1)
        self.assertEqual(len(face_identity.ui_rows(self.redis)), 1)

    def test_people_ui_has_a_compact_faces_tab_and_management_actions(self):
        app = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

        self.assertIn('data-people-tab="faces"', app)
        self.assertIn("people_face_move_images", app)
        self.assertIn("people_face_remove_images", app)
        self.assertIn("people_face_merge", app)
        self.assertIn(".people-face-grid", styles)
        self.assertIn(".people-face-gallery", styles)


if __name__ == "__main__":
    unittest.main()
