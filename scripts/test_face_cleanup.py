"""Cleanup preserves photos and uses only references confirmed by a person."""
import copy
import unittest

from tater.identity.cleanup import plan_cleanup
from tater.identity import faces


def vector(x, y=0):
    return [float(x), float(y), *([0.0] * 126)]


def observation(token, *, trusted=False):
    return {
        "id": token, "event_id": "event_" + token, "face_b64": "ZmFjZQ==",
        "embedding": [1.0, *([0.0] * 511)],
        "embedding_model_signature": "facenet512|cosine|512",
        "reference_status": "trusted" if trusted else "provisional",
        "embedding_model": {"model_id": "facenet512", "model_name": "Facenet512", "embedding_dimensions": 512},
    }


def profile(token, name, *rows):
    return {
        "id": token, "name": name, "person_id": "person_" + token,
        "embedding_model_signature": "facenet512|cosine|512", "observations": list(rows),
    }


class FaceCleanupTests(unittest.TestCase):
    def setUp(self):
        self.rows = {
            "a": profile("a", "Alice", observation("a_ref"), observation("wrong"), observation("failed")),
            "b": profile("b", "Bob", observation("b_ref")),
        }
        self.embeddings = {"a_ref": vector(1), "b_ref": vector(-1), "wrong": vector(-.99, .1)}
        self.confirmed = {"a": ["a_ref"], "b": ["b_ref"]}

    def test_reassigns_bad_match_and_updates_event_without_losing_images(self):
        original = copy.deepcopy(self.rows)
        result = plan_cleanup(self.rows, self.embeddings, self.confirmed)
        self.assertEqual(self.rows, original)
        self.assertEqual(result["events"]["event_wrong"], ["b"])
        self.assertEqual(result["identities"]["a"]["person_id"], "person_a")
        captures = [row for identity in result["identities"].values() for row in identity["observations"]]
        self.assertCountEqual([row["id"] for row in captures], ["a_ref", "b_ref", "wrong", "failed"])
        self.assertTrue(all(row["face_b64"] == "ZmFjZQ==" for row in captures))
        self.assertEqual(result["summary"]["needs_clearer_photo"], 1)
        failed_profile = result["identities"][result["events"]["event_failed"][0]]
        self.assertFalse(failed_profile.get("name"))
        self.assertFalse(failed_profile.get("person_id"))

    def test_rejected_photo_cannot_return_to_rejected_person(self):
        self.embeddings["wrong"] = vector(.99, .1)
        result = plan_cleanup(self.rows, self.embeddings, self.confirmed, {"a": ["wrong"]})
        self.assertNotEqual(result["events"]["event_wrong"], ["a"])
        self.assertNotEqual(result["events"]["event_wrong"], ["b"])

    def test_old_trust_is_not_used_as_ground_truth(self):
        self.rows["a"]["observations"][1]["reference_status"] = "trusted"
        result = plan_cleanup(self.rows, self.embeddings, self.confirmed)
        bob = result["identities"]["b"]
        wrong = next(row for row in bob["observations"] if row["id"] == "wrong")
        self.assertEqual(wrong["reference_status"], "provisional")
        signature = faces._embedding_model_signature(faces.face_id_runtime.embedding_model_metadata())
        self.assertEqual(len(faces.trusted_reference_embeddings(bob, signature)), 1)
        self.assertEqual(len(wrong["embedding"]), 512)
        self.assertEqual(len(faces.observation_embedding(wrong, signature)), 128)

    def test_missing_or_conflicting_reference_aborts(self):
        with self.assertRaises(ValueError):
            plan_cleanup(self.rows, self.embeddings, {"a": ["a_ref"]})
        with self.assertRaises(ValueError):
            plan_cleanup(self.rows, self.embeddings, {"a": ["absent"], "b": ["b_ref"]})
        with self.assertRaises(ValueError):
            plan_cleanup(self.rows, self.embeddings, {"a": ["a_ref"], "b": ["a_ref"]})
        with self.assertRaises(ValueError):
            plan_cleanup(self.rows, self.embeddings, self.confirmed, {"a": ["a_ref"]})

    def test_sface_accepts_reference_cutoff_with_clear_person_margin(self):
        identity = {"id": "a", "name": "Alice", "observations": [{
            **observation("a_ref", trusted=True), "embedding": vector(1),
            "embedding_model_signature": "sface|cosine|128|revision",
        }]}
        query = vector(.4, (.84 ** .5))  # cosine distance 0.60
        self.assertEqual(faces.match_identity({"a": identity}, query, model_signature="sface|cosine|128|revision")[0], "a")
        other = copy.deepcopy(identity)
        other["name"] = "Bob"
        self.assertEqual(faces.match_identity({"a": identity, "b": other}, query, model_signature="sface|cosine|128|revision")[0], "")
        identity["name"] = ""
        self.assertEqual(faces.match_identity({"a": identity}, query, model_signature="sface|cosine|128|revision")[0], "")


if __name__ == '__main__':
    unittest.main()
