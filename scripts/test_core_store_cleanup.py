from __future__ import annotations

import ast
import fnmatch
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from typing import Any, Dict
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
CORE_STORE_PATH = ROOT / "tateros" / "core_store.py"
APP_PATH = ROOT / "tateros_app.py"


class _FakeRedis:
    def __init__(self, values: Dict[str, Any] | None = None) -> None:
        self.values = dict(values or {})
        self.scan_patterns: list[str] = []

    def exists(self, key: str) -> int:
        return int(key in self.values)

    def get(self, key: str) -> Any:
        return self.values.get(key)

    def set(self, key: str, value: Any) -> None:
        self.values[key] = value

    def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if key in self.values:
                deleted += 1
                self.values.pop(key, None)
        return deleted

    def scan_iter(self, *, match: str, count: int = 10):
        del count
        self.scan_patterns.append(match)
        for key in list(self.values):
            if fnmatch.fnmatchcase(key, match):
                yield key


def _load_core_store(fake_redis: _FakeRedis):
    helpers = types.ModuleType("helpers")
    helpers.redis_client = fake_redis
    module_name = f"test_core_store_{id(fake_redis)}"
    spec = importlib.util.spec_from_file_location(module_name, CORE_STORE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load core_store.py")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {"helpers": helpers}):
        spec.loader.exec_module(module)
    return module


class CoreRedisCleanupTests(unittest.TestCase):
    def test_custom_core_namespace_is_deleted_without_a_declaration(self) -> None:
        redis = _FakeRedis(
            {
                "custom_hud_core_settings": "settings",
                "custom_hud_core_running": "true",
                "tater:cooldown:custom_hud_core": "1",
                "custom_hud:profiles": "profiles",
                "custom_hud:layout:kitchen": "layout",
                "custom_hud:tts:cache": "audio",
                "custom_hud_core_extra_core_settings": "other core",
                "verba_enabled": "shared verba state",
                "verba_settings:Custom HUD": "shared verba settings",
                "unrelated:value": "keep",
            }
        )
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("custom_hud")

        self.assertTrue(ok, message)
        self.assertEqual(
            redis.values,
            {
                "custom_hud_core_running": "false",
                "custom_hud_core_extra_core_settings": "other core",
                "verba_enabled": "shared verba state",
                "verba_settings:Custom HUD": "shared verba settings",
                "unrelated:value": "keep",
            },
        )
        self.assertIn("Deleted 5 Redis key(s)", message)

    def test_personal_core_removes_profiles_history_and_processing_state(self) -> None:
        personal_keys = {
            "personal:stats:core",
            "personal:accounts",
            "personal:profiles",
            "personal:account_people",
            "personal:profile:primary",
            "personal:email_history:primary",
            "personal:cursor_uid:primary",
            "personal:processed_msg:primary",
            "personal:notify_sent:delivery",
        }
        redis = _FakeRedis(
            {
                **{key: "private" for key in personal_keys},
                "personal_core_settings": "credentials",
                "personal_core_running": "true",
                "other_core:profile": "keep",
            }
        )
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("personal")

        self.assertTrue(ok, message)
        self.assertTrue(personal_keys.isdisjoint(redis.values))
        self.assertNotIn("personal_core_settings", redis.values)
        self.assertEqual(redis.values["personal_core_running"], "false")
        self.assertEqual(redis.values["other_core:profile"], "keep")

    def test_memory_core_removes_current_and_legacy_personal_memory(self) -> None:
        memory_keys = {
            "mem:user:webui:primary",
            "mem:room:webui:chat",
            "mem:cursor:webui:chat",
            "mem:identity_alias:telegram:123",
            "mem:identity_name:alice",
            "mem:stats:memory_core",
            "mem:ui:memory_core:last_tool",
            "tater:memory:user:legacy",
            "tater:room_label:webui:chat",
            "tater:user_label:webui:primary",
        }
        redis = _FakeRedis(
            {
                **{key: "private" for key in memory_keys},
                "memory_core_settings": "settings",
                "memory_core_running": "true",
                "tater:telegram:chat_lookup": "shared",
                "webui:chat_history": "shared",
            }
        )
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("memory")

        self.assertTrue(ok, message)
        self.assertTrue(memory_keys.isdisjoint(redis.values))
        self.assertNotIn("memory_core_settings", redis.values)
        self.assertEqual(redis.values["memory_core_running"], "false")
        self.assertEqual(redis.values["tater:telegram:chat_lookup"], "shared")
        self.assertEqual(redis.values["webui:chat_history"], "shared")

    def test_legacy_exact_keys_are_removed_without_touching_adjacent_core_data(self) -> None:
        redis = _FakeRedis(
            {
                "music_core_settings": "settings",
                "music_core_running": "true",
                "music_core_listening_history_v1": "history",
                "tater_tube_activity_feed_v1": "activity",
                "tater_tube_core_context": "keep",
            }
        )
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("music")

        self.assertTrue(ok, message)
        self.assertNotIn("music_core_listening_history_v1", redis.values)
        self.assertNotIn("tater_tube_activity_feed_v1", redis.values)
        self.assertEqual(redis.values["tater_tube_core_context"], "keep")

    def test_tater_tube_legacy_keys_are_removed_but_shared_music_data_is_preserved(self) -> None:
        redis = _FakeRedis(
            {
                "tater_tube_core_settings": "settings",
                "tater_tube_core_running": "true",
                "tater_tube_core_context": "context",
                "tater_tube_core_recommendations": "recommendations",
                "tater_tube_core_main_menu_message_v1": "message",
                "music_core_listening_history_v1": "shared",
            }
        )
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("tater_tube")

        self.assertTrue(ok, message)
        self.assertEqual(
            redis.values,
            {
                "tater_tube_core_running": "false",
                "music_core_listening_history_v1": "shared",
            },
        )

    def test_cleanup_has_no_key_count_cap(self) -> None:
        redis = _FakeRedis({f"custom_hud:item:{index}": str(index) for index in range(501)})
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("custom_hud")

        self.assertTrue(ok, message)
        self.assertEqual(redis.values, {"custom_hud_core_running": "false"})
        self.assertIn("Deleted 501 Redis key(s)", message)

    def test_invalid_core_id_cannot_become_a_scan_pattern(self) -> None:
        redis = _FakeRedis({"personal:profile:primary": "keep"})
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("*")

        self.assertFalse(ok)
        self.assertIn("Invalid core id", message)
        self.assertEqual(redis.scan_patterns, [])
        self.assertEqual(redis.values, {"personal:profile:primary": "keep"})

    def test_generic_platform_namespace_is_not_inferred_from_a_core_id(self) -> None:
        redis = _FakeRedis(
            {
                "tater:hydra:ledger:turn": "shared",
                "tater_core:owned": "remove",
                "tater_core_running": "true",
            }
        )
        store = _load_core_store(redis)

        ok, message = store.clear_core_redis_data("tater")

        self.assertTrue(ok, message)
        self.assertEqual(redis.values["tater:hydra:ledger:turn"], "shared")
        self.assertNotIn("tater_core:owned", redis.values)
        self.assertEqual(redis.values["tater_core_running"], "false")


class _App:
    def post(self, _path: str):
        return lambda function: function


class _HTTPException(Exception):
    def __init__(self, *, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _ShopRemoveRequest:
    pass


def _load_remove_core(namespace: Dict[str, Any]):
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "remove_core"
    )
    module_tree = ast.Module(body=[function], type_ignores=[])
    ast.fix_missing_locations(module_tree)
    exec(compile(module_tree, str(APP_PATH), "exec"), namespace)
    return namespace["remove_core"]


class CoreRemovalEndpointTests(unittest.TestCase):
    def _namespace(self, *, cleanup_ok: bool = True, stop_running: bool = False):
        calls: list[str] = []

        class Store:
            @staticmethod
            def _is_reserved_builtin_core_id(_core_id: str) -> bool:
                return False

            @staticmethod
            def _safe_core_file_path(_core_id: str) -> str:
                return "cores/personal_core.py"

            @staticmethod
            def _core_module_key(_core_id: str) -> str:
                return "personal_core"

            @staticmethod
            def clear_core_redis_data(_core_id: str, *, module_key: str):
                calls.append(f"cleanup:{module_key}")
                return cleanup_ok, "cleanup result"

            @staticmethod
            def uninstall_core_file(_core_id: str):
                calls.append("uninstall")
                return True, "removed core"

        class Runtime:
            @staticmethod
            def is_running(_module_key: str) -> bool:
                return stop_running

            @staticmethod
            def stop(_module_key: str):
                calls.append("stop")
                return {"running": False}

        redis = _FakeRedis()
        namespace: Dict[str, Any] = {
            "Any": Any,
            "Dict": Dict,
            "HTTPException": _HTTPException,
            "ShopRemoveRequest": _ShopRemoveRequest,
            "app": _App(),
            "core_runtime": Runtime(),
            "core_store_module": Store(),
            "redis_client": redis,
        }
        return _load_remove_core(namespace), calls, redis

    def test_cleanup_finishes_before_the_core_file_is_removed(self) -> None:
        remove_core, calls, redis = self._namespace(stop_running=True)
        payload = types.SimpleNamespace(id="personal", purge_redis=True)

        result = remove_core(payload)

        self.assertEqual(calls, ["stop", "cleanup:personal_core", "uninstall"])
        self.assertEqual(redis.values["personal_core_running"], "false")
        self.assertIn("cleanup result", result["message"])

    def test_cleanup_failure_keeps_the_core_file_installed(self) -> None:
        remove_core, calls, _redis = self._namespace(cleanup_ok=False)
        payload = types.SimpleNamespace(id="personal", purge_redis=True)

        with self.assertRaises(_HTTPException) as raised:
            remove_core(payload)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(calls, ["cleanup:personal_core"])
        self.assertNotIn("uninstall", calls)

    def test_reserved_core_is_rejected_before_stop_or_cleanup(self) -> None:
        remove_core, calls, _redis = self._namespace(stop_running=True)
        remove_core.__globals__["core_store_module"]._is_reserved_builtin_core_id = lambda _core_id: True
        payload = types.SimpleNamespace(id="voice", purge_redis=True)

        with self.assertRaises(_HTTPException) as raised:
            remove_core(payload)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
