from __future__ import annotations

import unittest

import people


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

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

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

    def scan_iter(self, match=None, count=None):
        del match, count
        return iter(())


class TaterOpenWebUiIdentityTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()

    def test_account_is_discovered_with_installation_scoped_id(self):
        row = people.register_tater_open_webui_identity(
            node_id="node-1",
            node_name="Kitchen WebUI",
            user_id="web-user-7",
            user_name="Ada",
            redis_client=self.redis,
        )

        self.assertEqual(row["external_id"], "node-1:web-user-7")
        identities = people.discovered_identities(self.redis)
        identity = next(item for item in identities if item["platform"] == "tater_open_webui")
        self.assertEqual(identity["label"], "Ada")
        self.assertIn("Kitchen WebUI", identity["source"])

    def test_linked_account_resolves_to_its_person_and_admin_status(self):
        people.register_tater_open_webui_identity(
            node_id="node-1",
            node_name="Kitchen WebUI",
            user_id="web-user-7",
            user_name="Ada",
            redis_client=self.redis,
        )
        person = people.create_person("Ada Lovelace", self.redis)
        people.update_person(person["id"], {"is_admin": True}, self.redis)
        people.attach_alias(
            person_id=person["id"],
            platform="tater_open_webui",
            external_id="node-1:web-user-7",
            label="Ada",
            kind="webui_user",
            redis_client=self.redis,
        )

        status = people.tater_open_webui_identity_status(
            node_id="node-1",
            node_name="Kitchen WebUI",
            user_id="web-user-7",
            user_name="Ada",
            redis_client=self.redis,
        )

        self.assertTrue(status["registered"])
        self.assertTrue(status["linked"])
        self.assertEqual(status["person"]["name"], "Ada Lovelace")
        self.assertTrue(status["person"]["is_admin"])

    def test_same_user_id_on_two_installations_stays_separate(self):
        first = people.tater_open_webui_external_id("node-1", "same-user")
        second = people.tater_open_webui_external_id("node-2", "same-user")
        self.assertNotEqual(first, second)

    def test_unlinked_account_can_be_forgotten(self):
        people.register_tater_open_webui_identity(
            node_id="node-1",
            node_name="Kitchen WebUI",
            user_id="web-user-7",
            user_name="Ada",
            redis_client=self.redis,
        )
        result = people.forget_discovered_identity(
            platform="tater_open_webui",
            external_id="node-1:web-user-7",
            redis_client=self.redis,
        )
        self.assertEqual(result["removed_rows"], 1)
        self.assertFalse(
            people.tater_open_webui_identity_status(
                node_id="node-1",
                node_name="Kitchen WebUI",
                user_id="web-user-7",
                user_name="Ada",
                redis_client=self.redis,
            )["registered"]
        )


if __name__ == "__main__":
    unittest.main()
