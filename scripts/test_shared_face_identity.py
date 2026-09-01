import base64
import json
import time
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

    def test_adaface_ir50_is_a_versioned_selectable_model(self):
        model = face_identity.face_id_runtime.selected_model(model_id="adaface_ir50_webface4m")
        metadata = face_identity.face_id_runtime.embedding_model_metadata(model_id=model["id"])

        self.assertEqual(model["label"], "AdaFace IR-50 · WebFace4M")
        self.assertEqual(model["match_threshold"], 0.40)
        self.assertEqual(metadata["embedding_dimensions"], 512)
        self.assertEqual(metadata["model_revision"], face_identity.face_id_runtime.ADAFACE_REVISION)

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

    def test_people_face_enrollment_saves_one_uploaded_face(self):
        person = people.create_person("Fred", self.redis)
        upload = {
            "filename": "fred.jpg",
            "content_type": "image/jpeg",
            "data_b64": base64.b64encode(b"jpeg").decode("ascii"),
        }

        with (
            patch.object(face_identity, "runtime_status", return_value={"enabled": True, "loaded": True}),
            patch.object(face_identity.face_id_runtime, "analyze_image", return_value=[self.detection([1.0, 0.0])]),
        ):
            result = people.handle_action(
                "people_face_enroll",
                {"values": {"person_id": person["id"], "face_image": upload}},
                self.redis,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["person_id"], person["id"])
        face = result["people"]["faces"][0]
        self.assertEqual(face["person_name"], "Fred")
        self.assertEqual(face["capture_count"], 1)
        self.assertEqual(face["gallery"][0]["source"]["kind"], "people_face_enrollment")

    def test_people_face_enrollment_rejects_multiple_faces_without_saving(self):
        person = people.create_person("Fred", self.redis)
        upload = {
            "filename": "group.jpg",
            "content_type": "image/jpeg",
            "data_b64": base64.b64encode(b"jpeg").decode("ascii"),
        }

        with (
            patch.object(face_identity, "runtime_status", return_value={"enabled": True, "loaded": True}),
            patch.object(
                face_identity.face_id_runtime,
                "analyze_image",
                return_value=[self.detection([1.0, 0.0]), self.detection([0.0, 1.0])],
            ),
        ):
            with self.assertRaisesRegex(ValueError, "More than one face"):
                people.handle_action(
                    "people_face_enroll",
                    {"values": {"person_id": person["id"], "face_image": upload}},
                    self.redis,
                )

        self.assertEqual(face_identity.identity_rows(self.redis), {})

    def test_people_face_enrollment_does_not_reassign_an_existing_face(self):
        fred = people.create_person("Fred", self.redis)
        wilma = people.create_person("Wilma", self.redis)
        identity = face_identity.record_detection(
            self.detection([1.0, 0.0]),
            event_id="existing-face",
            redis_client=self.redis,
        )
        face_identity.save_profile(identity["id"], person_id=wilma["id"], redis_client=self.redis)
        upload = {
            "filename": "fred.jpg",
            "content_type": "image/jpeg",
            "data_b64": base64.b64encode(b"jpeg").decode("ascii"),
        }

        with (
            patch.object(face_identity, "runtime_status", return_value={"enabled": True, "loaded": True}),
            patch.object(face_identity.face_id_runtime, "analyze_image", return_value=[self.detection([1.0, 0.0])]),
        ):
            with self.assertRaisesRegex(ValueError, "already linked to Wilma"):
                people.handle_action(
                    "people_face_enroll",
                    {"values": {"person_id": fred["id"], "face_image": upload}},
                    self.redis,
                )

        saved = face_identity.identity_rows(self.redis)[identity["id"]]
        self.assertEqual(saved["person_id"], wilma["id"])
        self.assertEqual(saved["observation_count"], 1)

    def test_spudlet_face_id_uses_hub_embeddings_with_local_people(self):
        person = people.create_person("Fred", self.redis)
        identity = face_identity.record_detection(
            self.detection([1.0, 0.0]),
            event_id="local-enrollment",
            redis_client=self.redis,
        )
        face_identity.save_profile(identity["id"], person_id=person["id"], redis_client=self.redis)
        remote_result = {
            "result": {
                "status": "embedded",
                "detections": [self.detection([0.999, 0.001])],
                "model": {
                    "model_name": "Facenet512",
                    "distance_metric": "cosine",
                    "embedding_dimensions": 2,
                    "match_threshold": 0.30,
                },
                "stored": False,
            }
        }

        with (
            patch.object(face_identity, "runtime_status", return_value={"enabled": True, "loaded": True}),
            patch.object(face_identity, "spud_link_should_use_hub", return_value=True),
            patch.object(face_identity, "spud_link_request_json", return_value=remote_result) as request,
        ):
            result = face_identity.recognize_image(
                b"jpeg",
                event_id="reachy-saw-fred",
                source={"device": "Reachy"},
                record=False,
                redis_client=self.redis,
            )

        self.assertEqual(result["status"], "recognized")
        self.assertEqual(result["people"], ["Fred"])
        self.assertEqual(result["person_ids"], [person["id"]])
        self.assertEqual(result["identity_owner"], "This Tater")
        self.assertEqual(result["loaded_on"], "Spud Hub")
        payload = request.call_args.kwargs["payload"]
        self.assertEqual(payload["operation"], "embed")
        self.assertNotIn("event_id", payload)
        self.assertNotIn("source", payload)
        self.assertNotIn("record", payload)

    def test_spudlet_manual_enrollment_embeds_once_and_saves_locally(self):
        person = people.create_person("Fred", self.redis)
        remote_result = {
            "result": {
                "status": "embedded",
                "detections": [self.detection([1.0, 0.0])],
                "model": {
                    "model_name": "Facenet512",
                    "distance_metric": "cosine",
                    "embedding_dimensions": 2,
                    "match_threshold": 0.30,
                },
                "stored": False,
            }
        }

        with (
            patch.object(face_identity, "runtime_status", return_value={"enabled": True, "loaded": True}),
            patch.object(face_identity, "spud_link_should_use_hub", return_value=True),
            patch.object(face_identity, "spud_link_request_json", return_value=remote_result) as request,
        ):
            result = face_identity.enroll_person_image(
                b"jpeg",
                person_id=person["id"],
                source={"kind": "people_face_enrollment"},
                redis_client=self.redis,
            )

        self.assertEqual(request.call_count, 1)
        self.assertEqual(result["person_id"], person["id"])
        self.assertEqual(result["routed_via"], "spud_link")
        saved = face_identity.identity_rows(self.redis)[result["identity_id"]]
        self.assertEqual(saved["person_id"], person["id"])
        self.assertEqual(saved["observations"][0]["source"]["kind"], "people_face_enrollment")

    def test_spudlet_backfills_linked_faces_when_hub_model_changes(self):
        person = people.create_person("Fred", self.redis)
        facenet_embedding = [1.0, *([0.0] * 511)]
        identity = face_identity.record_detection(
            self.detection(facenet_embedding),
            event_id="local-facenet-enrollment",
            redis_client=self.redis,
        )
        face_identity.save_profile(identity["id"], person_id=person["id"], redis_client=self.redis)
        adaface_embedding = [0.0, 1.0, *([0.0] * 510)]
        remote_result = {
            "result": {
                "status": "embedded",
                "detections": [self.detection(adaface_embedding)],
                "model": face_identity.face_id_runtime.embedding_model_metadata(
                    self.redis,
                    model_id="adaface_ir50_webface4m",
                ),
                "stored": False,
            }
        }

        with (
            patch.object(face_identity, "runtime_status", return_value={"enabled": True, "loaded": True}),
            patch.object(face_identity, "spud_link_should_use_hub", return_value=True),
            patch.object(face_identity, "spud_link_request_json", return_value=remote_result) as request,
        ):
            result = face_identity.recognize_image(
                b"jpeg",
                event_id="hub-now-uses-adaface",
                record=False,
                redis_client=self.redis,
            )

        self.assertEqual(result["status"], "recognized")
        self.assertEqual(result["people"], ["Fred"])
        self.assertEqual(request.call_count, 2)
        self.assertEqual(
            request.call_args_list[1].kwargs["payload"]["model_id"],
            "adaface_ir50_webface4m",
        )

    def test_face_matching_does_not_mix_incompatible_embedding_models(self):
        identity = face_identity.record_detection(
            self.detection([1.0, 0.0]),
            event_id="facenet-face",
            redis_client=self.redis,
        )
        incompatible = face_identity._annotate_detection(
            self.detection([1.0, 0.0]),
            {
                "model_name": "DifferentFaceModel",
                "distance_metric": "cosine",
                "embedding_dimensions": 2,
                "match_threshold": 0.30,
            },
        )

        matched_id, _distance = face_identity.match_identity(
            face_identity.identity_rows(self.redis),
            incompatible["embedding"],
            model_signature=incompatible["embedding_model_signature"],
        )

        self.assertTrue(identity["embedding_model_signature"])
        self.assertEqual(matched_id, "")

    def test_model_switch_caches_new_embeddings_and_keeps_facenet_rollback(self):
        person = people.create_person("Fred", self.redis)
        facenet_embedding = [1.0, *([0.0] * 511)]
        identity = face_identity.record_detection(
            self.detection(facenet_embedding),
            event_id="facenet-enrollment",
            redis_client=self.redis,
        )
        face_identity.save_profile(identity["id"], person_id=person["id"], redis_client=self.redis)
        self.redis.set(face_identity.face_id_runtime.ENABLED_KEY, "true")
        adaface_embedding = [0.0, 1.0, *([0.0] * 510)]

        def activate_model(client, model_id, *, load=True):
            del load
            client.set(face_identity.face_id_runtime.MODEL_KEY, model_id)
            return {"model_id": model_id}

        with (
            patch.object(
                face_identity.face_id_runtime,
                "analyze_image",
                return_value=[self.detection(adaface_embedding)],
            ),
            patch.object(face_identity.face_id_runtime, "set_model", side_effect=activate_model),
        ):
            face_identity.start_model_switch(self.redis, "adaface_ir50_webface4m")
            deadline = time.monotonic() + 3.0
            while face_identity.model_switch_status(self.redis).get("state") not in {"complete", "error"} and time.monotonic() < deadline:
                time.sleep(0.01)

        state = face_identity.model_switch_status(self.redis)
        self.assertEqual(state["state"], "complete")
        saved = face_identity.identity_rows(self.redis)[identity["id"]]
        adaface_model = face_identity.face_id_runtime.embedding_model_metadata(
            self.redis,
            model_id="adaface_ir50_webface4m",
        )
        adaface_signature = face_identity._embedding_model_signature(adaface_model, dimensions=512)
        facenet_signature = identity["embedding_model_signature"]

        self.assertIn(adaface_signature, saved["embedding_profiles"])
        self.assertEqual(
            face_identity.match_identity(
                {identity["id"]: saved},
                adaface_embedding,
                threshold=0.40,
                model_signature=adaface_signature,
            )[0],
            identity["id"],
        )
        self.assertEqual(
            face_identity.match_identity(
                {identity["id"]: saved},
                facenet_embedding,
                threshold=0.30,
                model_signature=facenet_signature,
            )[0],
            identity["id"],
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
        saved = face_identity.identity_rows(self.redis)[identity["id"]]
        self.assertEqual(face_identity.observations(saved), [])
        self.assertEqual(face_identity.reference_embeddings(saved), [])
        self.assertNotIn("centroid", saved)
        self.assertNotIn("reference_centroids", saved)
        self.assertNotIn("face_b64", saved)

    def test_legacy_image_less_anchor_vectors_are_scrubbed_on_read(self):
        self.redis.hashes[face_identity.SHARED_IDENTITIES_KEY] = {
            "face_legacy": json.dumps(
                {
                    "id": "face_legacy",
                    "name": "Legacy",
                    "anchor_references": [[1.0, 0.0]],
                    "centroid": [1.0, 0.0],
                    "centroid_count": 1,
                    "reference_centroids": [[1.0, 0.0]],
                    "face_b64": "ZmFjZQ==",
                    "observations": [
                        {
                            "id": "observation_without_image",
                            "embedding": [1.0, 0.0],
                            "seen_at": "2026-08-01T12:00:00Z",
                        }
                    ],
                }
            )
        }

        identity = face_identity.identity_rows(self.redis)["face_legacy"]

        self.assertNotIn("anchor_references", identity)
        self.assertNotIn("centroid", identity)
        self.assertNotIn("reference_centroids", identity)
        self.assertNotIn("face_b64", identity)
        self.assertEqual(face_identity.observations(identity), [])
        self.assertEqual(face_identity.reference_embeddings(identity), [])
        persisted = json.loads(self.redis.hashes[face_identity.SHARED_IDENTITIES_KEY]["face_legacy"])
        self.assertNotIn("anchor_references", persisted)

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
        self.assertIn('data-people-tab="identities"', app)
        self.assertIn("people_face_move_images", app)
        self.assertIn("people_face_remove_images", app)
        self.assertIn("people_face_merge", app)
        self.assertIn("people_face_enroll", app)
        self.assertIn("data-people-face-enroll-open", app)
        self.assertIn('id="people-face-enroll-modal"', app)
        self.assertIn("collectCoreManagerValuesWithFiles(form)", app)
        self.assertIn("bindCoreCameraCaptureFields(body)", app)
        self.assertIn('id="people-face-review-modal"', app)
        self.assertIn("data-people-face-review", app)
        self.assertIn('aria-pressed="false"', app)
        self.assertIn("data-people-face-selection-count", app)
        self.assertIn("data-people-face-select-all", app)
        self.assertIn("Permanently Delete", app)
        self.assertNotIn('type="checkbox" value="${escapeHtml(observationId)}" data-people-face-observation', app)
        self.assertIn(".people-face-grid", styles)
        self.assertIn(".people-face-gallery", styles)
        self.assertIn(".people-face-review-dialog", styles)
        self.assertIn(".people-face-enroll-dialog", styles)
        self.assertIn('.people-face-capture[aria-pressed="true"]', styles)
        self.assertIn(".people-face-selection-mark", styles)
        self.assertIn(".people-subtabs::-webkit-scrollbar", styles)
        self.assertIn("flex-wrap: nowrap;", styles)
        self.assertIn("white-space: nowrap;", styles)

    def test_face_id_settings_support_safe_model_switching(self):
        app = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('id="set_face_id_model"', app)
        self.assertIn("adaface_ir50_webface4m", app)
        self.assertIn("Preparing saved faces for the new model", app)
        self.assertIn("face_id_model:", app)


class PeopleResolutionTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.redis.values[people.PEOPLE_STORE_KEY] = json.dumps(
            {
                "people": [
                    {
                        "id": "person_fred",
                        "display_name": "Fred",
                        "instructions": "Use Fred's preferred units.",
                        "aliases": [],
                    },
                    {
                        "id": "person_wilma",
                        "display_name": "Wilma",
                        "aliases": [
                            {
                                "platform": "discord",
                                "external_id": "wilma-discord",
                                "label": "Wilma",
                            }
                        ],
                    },
                ]
            }
        )

    def test_resolves_existing_person_id_before_aliases(self):
        resolved = people.resolve_person(
            platform="discord",
            origin={"person_id": "person_fred", "user_id": "wilma-discord"},
            redis_client=self.redis,
        )

        self.assertTrue(resolved["matched"])
        self.assertEqual(resolved["match_type"], "person_id")
        self.assertEqual(resolved["person_id"], "person_fred")
        self.assertEqual(resolved["master_user_id"], "person_fred")
        self.assertEqual(resolved["display_name"], "Fred")
        self.assertEqual(resolved["instructions"], "Use Fred's preferred units.")

    def test_resolves_existing_master_user_id(self):
        resolved = people.resolve_person(
            platform="voice_core",
            origin={"master_user_id": "person_fred"},
            redis_client=self.redis,
        )

        self.assertTrue(resolved["matched"])
        self.assertEqual(resolved["match_type"], "master_user_id")
        self.assertEqual(resolved["person_id"], "person_fred")

    def test_apply_resolution_preserves_trusted_identity(self):
        origin = {"person_id": "person_fred", "unlocked_by_name": "Fred"}

        resolved = people.apply_resolution_to_origin(
            platform="voice_core",
            origin=origin,
            redis_client=self.redis,
        )

        self.assertTrue(resolved["matched"])
        self.assertEqual(origin["person_id"], "person_fred")
        self.assertEqual(origin["master_user_id"], "person_fred")
        self.assertEqual(origin["person_name"], "Fred")
        self.assertEqual(origin["person_instructions"], "Use Fred's preferred units.")

    def test_unknown_person_id_can_still_resolve_by_alias(self):
        resolved = people.resolve_person(
            platform="discord",
            origin={"person_id": "person_missing", "user_id": "wilma-discord"},
            redis_client=self.redis,
        )

        self.assertTrue(resolved["matched"])
        self.assertEqual(resolved["match_type"], "alias")
        self.assertEqual(resolved["person_id"], "person_wilma")


if __name__ == "__main__":
    unittest.main()
