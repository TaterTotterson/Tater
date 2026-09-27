from __future__ import annotations

import ast
import asyncio
from pathlib import Path
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def _load_mapping_helpers():
    source = (ROOT / "integration_runtime.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    names = {
        "_unifi_sensor_event_state",
        "_unifi_timestamp_ms",
        "_unifi_sensor_opened",
        "_unifi_sensor_event_match",
        "_unifi_resolve_sensor_event_item",
    }
    functions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    cache = {}

    async def run_background(function, *args, **kwargs):
        return function(*args, **kwargs)

    namespace = {
        "_text": lambda value: str(value or "").strip(),
        "_as_float": lambda value, default=0.0: float(value or default),
        "_as_bool": lambda value, default=False: value if isinstance(value, bool) else str(value).lower() in {"1", "true", "yes", "on"},
        "_unifi_sensor_event_cache_get": lambda event_id: cache.get(str(event_id or "").lower(), ""),
        "_unifi_sensor_event_cache_set": lambda event_id, sensor_id: cache.__setitem__(str(event_id).lower(), str(sensor_id).lower()),
        "run_background": run_background,
        "logger": mock.Mock(),
        "asyncio": asyncio,
    }
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.Module(body=[future, *functions], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "<unifi-sensor-event-mapping>", "exec"), namespace)
    return namespace, cache


class UnifiSensorEventMappingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.helpers, self.cache = _load_mapping_helpers()

    def test_matches_sensor_by_edge_state_and_protect_timestamp(self) -> None:
        match = self.helpers["_unifi_sensor_event_match"](
            {"type": "sensorOpened", "start": 1_790_423_123_456},
            [
                {"id": "garage", "isOpened": False, "openStatusChangedAt": 1_790_423_123_456},
                {"id": "back-door", "isOpened": True, "openStatusChangedAt": 1_790_423_123_456},
            ],
        )

        self.assertEqual(match["id"], "back-door")

    def test_ambiguous_timestamp_does_not_guess(self) -> None:
        match = self.helpers["_unifi_sensor_event_match"](
            {"type": "sensorClosed", "start": 1_790_423_123_456},
            [
                {"id": "front", "isOpened": False, "openStatusChangedAt": 1_790_423_123_456},
                {"id": "back", "isOpened": False, "openStatusChangedAt": 1_790_423_123_500},
            ],
        )

        self.assertIsNone(match)

    async def test_resolver_adds_sensor_id_and_reuses_event_id_mapping(self) -> None:
        protect = mock.Mock()
        protect.list_unifi_sensors.return_value = [
            {
                "id": "sensor-back-door",
                "name": "Back Door",
                "isOpened": True,
                "openStatusChangedAt": 1_790_423_123_456,
            }
        ]
        event = {
            "id": "protect-event-1",
            "type": "sensorOpened",
            "device": "camera-back-porch",
            "start": 1_790_423_123_456,
        }

        first = await self.helpers["_unifi_resolve_sensor_event_item"](event, protect)
        repeated = await self.helpers["_unifi_resolve_sensor_event_item"](event, protect)

        self.assertEqual(first["sensor"], "sensor-back-door")
        self.assertEqual(first["sensorName"], "Back Door")
        self.assertEqual(repeated["sensor"], "sensor-back-door")
        self.assertEqual(protect.list_unifi_sensors.call_count, 1)


if __name__ == "__main__":
    unittest.main()
