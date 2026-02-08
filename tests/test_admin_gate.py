import unittest

from admin_gate import (
    CREATION_GATE_KEY,
    REDIS_KEY,
    DEFAULT_ADMIN_ONLY_PLUGINS,
    get_admin_only_plugins,
    is_agent_lab_creation_admin_gated,
    is_agent_lab_creation_tool,
    normalize_admin_list,
)


class _FakeRedis:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key):
        return self.values.get(key)


class AdminGateTests(unittest.TestCase):
    def test_normalize_admin_list_handles_json_and_case(self):
        raw = '["Broadcast", "ha_control", ""]'
        self.assertEqual(normalize_admin_list(raw), {"broadcast", "ha_control"})

    def test_get_admin_only_plugins_defaults_when_missing(self):
        fake = _FakeRedis({})
        defaults = get_admin_only_plugins(fake)
        self.assertEqual(defaults, set(DEFAULT_ADMIN_ONLY_PLUGINS))
        self.assertIn("get_notifications", defaults)

    def test_get_admin_only_plugins_uses_saved_value(self):
        fake = _FakeRedis({REDIS_KEY: '["custom_tool","HA_CONTROL"]'})
        self.assertEqual(get_admin_only_plugins(fake), {"custom_tool", "ha_control"})

    def test_creation_tool_match_is_case_insensitive(self):
        self.assertTrue(is_agent_lab_creation_tool("create_plugin"))
        self.assertTrue(is_agent_lab_creation_tool("CREATE_PLATFORM"))
        self.assertFalse(is_agent_lab_creation_tool("search_web"))

    def test_creation_gate_defaults_off(self):
        fake = _FakeRedis({})
        self.assertFalse(is_agent_lab_creation_admin_gated(fake))

    def test_creation_gate_true_values(self):
        for value in ("1", "true", "yes", "on", True):
            with self.subTest(value=value):
                fake = _FakeRedis({CREATION_GATE_KEY: value})
                self.assertTrue(is_agent_lab_creation_admin_gated(fake))

    def test_creation_gate_false_values(self):
        for value in ("0", "false", "no", "off", False, "unexpected"):
            with self.subTest(value=value):
                fake = _FakeRedis({CREATION_GATE_KEY: value})
                self.assertFalse(is_agent_lab_creation_admin_gated(fake))


if __name__ == "__main__":
    unittest.main()
