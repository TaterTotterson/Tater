import json
import time
import unittest
from unittest.mock import patch

from kernel_tools import (
    memory_delete,
    memory_explain,
    memory_get,
    memory_list,
    memory_search,
    memory_set,
    truth_get_last,
    truth_list,
)
from truth_store import save_truth_snapshot


class _FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.lists = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value):
        self.kv[key] = value
        return True

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, *args, **kwargs):
        h = self.hashes.setdefault(key, {})
        mapping = kwargs.get("mapping")
        if isinstance(mapping, dict):
            for field, value in mapping.items():
                h[str(field)] = value
            return len(mapping)
        if len(args) == 2:
            field, value = args
            h[str(field)] = value
            return 1
        return 0

    def hdel(self, key, *fields):
        h = self.hashes.get(key, {})
        deleted = 0
        for field in fields:
            f = str(field)
            if f in h:
                del h[f]
                deleted += 1
        if not h and key in self.hashes:
            del self.hashes[key]
        return deleted

    def rpush(self, key, value):
        lst = self.lists.setdefault(key, [])
        lst.append(value)
        return len(lst)

    def ltrim(self, key, start, end):
        self.lists[key] = self._slice(self.lists.get(key, []), start, end)
        return True

    def lrange(self, key, start, end):
        return self._slice(self.lists.get(key, []), start, end)

    @staticmethod
    def _slice(items, start, end):
        n = len(items)
        if n == 0:
            return []
        if start < 0:
            start = n + start
        if end < 0:
            end = n + end
        start = max(0, start)
        if start >= n:
            return []
        end = min(end, n - 1)
        if end < start:
            return []
        return list(items[start : end + 1])


class KernelToolsMemoryTests(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeRedis()
        self.patcher = patch("kernel_tools.redis_client", self.fake)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_memory_set_requires_explicit_intent_by_default(self):
        result = memory_set(
            {"pref.city": "Seattle"},
            scope="global",
            request_text="set my city to Seattle",
        )
        self.assertFalse(result.get("ok"), result)
        self.assertIn("blocked", result.get("error", "").lower())
        self.assertTrue(result.get("needs"), result)

    def test_memory_set_accepts_explicit_phrase_yes_save_that(self):
        result = memory_set(
            {"pref.city": "Seattle"},
            scope="global",
            request_text="yes save that",
        )
        self.assertTrue(result.get("ok"), result)
        self.assertIn("pref.city", result.get("written", []))

    def test_memory_set_get_list_delete_roundtrip(self):
        set_result = memory_set(
            {"pref.city": "Seattle", "device.kitchen": "media_player.sonos_kitchen"},
            scope="global",
            confirmed=True,
        )
        self.assertTrue(set_result.get("ok"), set_result)
        self.assertEqual(set_result.get("count"), 2)

        get_result = memory_get(keys=["pref.city"], scope="global")
        self.assertTrue(get_result.get("ok"), get_result)
        self.assertEqual(get_result["values"]["pref.city"], "Seattle")

        list_result = memory_list(scope="global", prefix="device.", limit=10)
        self.assertTrue(list_result.get("ok"), list_result)
        self.assertEqual(list_result.get("total_count"), 1)
        self.assertEqual(list_result.get("items")[0]["key"], "device.kitchen")

        delete_result = memory_delete(keys=["device.kitchen"], scope="global")
        self.assertTrue(delete_result.get("ok"), delete_result)
        self.assertEqual(delete_result.get("deleted"), 1)

        verify = memory_get(keys=["device.kitchen"], scope="global")
        self.assertTrue(verify.get("ok"), verify)
        self.assertEqual(verify.get("values"), {})
        self.assertIn("device.kitchen", verify.get("missing", []))

    def test_memory_set_applies_ttl_only_to_volatile_keys(self):
        self.fake.set("tater:memory:default_ttl_sec", "60")
        set_result = memory_set(
            {"pref.city": "Seattle", "last.weather_city": "Seattle"},
            scope="global",
            confirmed=True,
        )
        self.assertTrue(set_result.get("ok"), set_result)
        self.assertIn("last.weather_city", set_result.get("ttl_applied", []))
        self.assertIn("pref.city", set_result.get("ttl_skipped", []))

        raw_pref = self.fake.hashes["tater:memory:global"]["pref.city"]
        raw_last = self.fake.hashes["tater:memory:global"]["last.weather_city"]
        pref_payload = json.loads(raw_pref)
        last_payload = json.loads(raw_last)
        self.assertIsNone(pref_payload.get("expires_at"))
        self.assertIsNotNone(last_payload.get("expires_at"))

    def test_memory_user_scope_can_use_origin_defaults(self):
        set_result = memory_set(
            {"pref.notifier": "discord:#alerts"},
            scope="user",
            request_text="remember this notifier target",
            origin={"user": "Alice"},
        )
        self.assertTrue(set_result.get("ok"), set_result)
        self.assertEqual(set_result.get("scope"), "user")

        get_result = memory_get(scope="user", user_id="Alice", keys=["pref.notifier"])
        self.assertTrue(get_result.get("ok"), get_result)
        self.assertEqual(get_result["values"]["pref.notifier"], "discord:#alerts")

    def test_memory_entries_expire_with_ttl(self):
        set_result = memory_set(
            {"temp.value": "short lived"},
            scope="room",
            platform="webui",
            room_id="chat",
            ttl_sec=5,
            confirmed=True,
        )
        self.assertTrue(set_result.get("ok"), set_result)

        key = "tater:memory:room:webui:chat"
        raw = self.fake.hashes[key]["temp.value"]
        payload = json.loads(raw)
        payload["expires_at"] = time.time() - 1
        self.fake.hashes[key]["temp.value"] = json.dumps(payload)

        list_result = memory_list(scope="room", platform="webui", room_id="chat", limit=10)
        self.assertTrue(list_result.get("ok"), list_result)
        self.assertEqual(list_result.get("count"), 0)
        self.assertEqual(list_result.get("total_count"), 0)

    def test_truth_get_last_and_truth_list(self):
        save_truth_snapshot(
            redis_client=self.fake,
            platform="webui",
            scope="chat",
            plugin_id="weather_brief",
            truth={"ok": True, "result_type": "action", "facts": {"temp": 71}},
            max_entries=50,
        )
        save_truth_snapshot(
            redis_client=self.fake,
            platform="webui",
            scope="chat",
            plugin_id="light_control",
            truth={"ok": True, "result_type": "action", "facts": {"state": "off"}},
            max_entries=50,
        )

        latest = truth_get_last(platform="webui", scope="chat")
        self.assertTrue(latest.get("ok"), latest)
        self.assertEqual(latest["entry"]["plugin_id"], "light_control")

        by_plugin = truth_get_last(platform="webui", scope="chat", plugin_id="weather_brief")
        self.assertTrue(by_plugin.get("ok"), by_plugin)
        self.assertEqual(by_plugin["entry"]["plugin_id"], "weather_brief")

        listing = truth_list(platform="webui", scope="chat", limit=2)
        self.assertTrue(listing.get("ok"), listing)
        self.assertEqual(listing.get("count"), 2)
        self.assertEqual(listing["entries"][0]["plugin_id"], "light_control")

    def test_memory_search_finds_memory_entries(self):
        memory_set(
            {"pref.city": "Seattle", "pref.notifier": "discord:#alerts"},
            scope="global",
            confirmed=True,
        )
        result = memory_search("seattle alerts", scope="global", include_truth=False, limit=5)
        self.assertTrue(result.get("ok"), result)
        self.assertGreaterEqual(result.get("count", 0), 1)
        self.assertEqual(result["results"][0]["kind"], "memory")
        keys = [item.get("key") for item in result.get("results", []) if item.get("kind") == "memory"]
        self.assertIn("pref.city", keys)

    def test_memory_search_can_include_truth_snapshots(self):
        save_truth_snapshot(
            redis_client=self.fake,
            platform="webui",
            scope="chat",
            plugin_id="rss_headlines",
            truth={"ok": True, "result_type": "research", "answer": "Top world headlines"},
            max_entries=50,
        )
        result = memory_search(
            "headlines",
            scope="auto",
            include_truth=True,
            origin={"platform": "webui", "scope": "chat"},
            limit=5,
        )
        self.assertTrue(result.get("ok"), result)
        truth_hits = [item for item in result.get("results", []) if item.get("kind") == "truth"]
        self.assertTrue(truth_hits, result)
        self.assertEqual(truth_hits[0].get("plugin_id"), "rss_headlines")

    def test_memory_explain_reports_active_scope_and_conflicts(self):
        memory_set({"pref.notifier": "discord:#general"}, scope="global", confirmed=True)
        memory_set({"pref.notifier": "discord:#ops"}, scope="user", user_id="alice", confirmed=True)
        memory_set(
            {"pref.notifier": "discord:#alerts"},
            scope="room",
            platform="webui",
            room_id="chat",
            confirmed=True,
        )

        result = memory_explain(
            "pref.notifier",
            scope="auto",
            origin={"platform": "webui", "user": "alice", "room": "chat"},
        )
        self.assertTrue(result.get("ok"), result)
        self.assertTrue(result.get("has_conflict"), result)
        active = result.get("active") or {}
        self.assertEqual(active.get("scope"), "room")
        self.assertEqual(active.get("value"), "discord:#alerts")
        self.assertGreaterEqual(len(result.get("entries") or []), 3)


if __name__ == "__main__":
    unittest.main()
