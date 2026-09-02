from __future__ import annotations

import ast
import inspect
from pathlib import Path
import time
import unittest
from typing import Any, Dict


RUNTIME_PATH = Path(__file__).resolve().parents[1] / "integration_runtime.py"


def _runtime_filter_namespace(module: Any) -> Dict[str, Any]:
    tree = ast.parse(RUNTIME_PATH.read_text(encoding="utf-8"), filename=str(RUNTIME_PATH))
    function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_integration_runtime_event_allowed"
    )

    class Logger:
        def warning(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    statuses: list[dict] = []
    namespace: Dict[str, Any] = {
        "Any": Any,
        "Dict": Dict,
        "inspect": inspect,
        "time": time,
        "logger": Logger(),
        "_RUNTIME_FILTER_ERROR_LOGGED_AT": {},
        "_runtime_provider_owner": lambda integration_id: str(integration_id),
        "_integration_module": lambda _integration_id: module,
        "_runtime_client": lambda client=None: client,
        "_text": lambda value: str(value or "").strip(),
        "_status_set": lambda _client, **fields: statuses.append(fields),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(RUNTIME_PATH), "exec"), namespace)
    namespace["statuses"] = statuses
    return namespace


class IntegrationRuntimeFilterTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_filter_hook_preserves_existing_behavior(self) -> None:
        namespace = _runtime_filter_namespace(object())

        allowed = await namespace["_integration_runtime_event_allowed"](
            "example", "state_changed", {"id": "one"}, object()
        )

        self.assertTrue(allowed)

    async def test_async_integration_hook_can_allow_or_block_runtime_data(self) -> None:
        class Integration:
            @staticmethod
            async def integration_runtime_event_allowed(*, kind, payload, client=None):
                del kind, client
                return payload.get("id") == "visible"

        namespace = _runtime_filter_namespace(Integration())
        check = namespace["_integration_runtime_event_allowed"]

        self.assertTrue(await check("example", "state_changed", {"id": "visible"}, object()))
        self.assertFalse(await check("example", "state_changed", {"id": "hidden"}, object()))

    async def test_filter_errors_fail_closed(self) -> None:
        class Integration:
            @staticmethod
            def integration_runtime_event_allowed(*, kind, payload, client=None):
                del kind, payload, client
                raise RuntimeError("provider unavailable")

        namespace = _runtime_filter_namespace(Integration())

        allowed = await namespace["_integration_runtime_event_allowed"](
            "example", "state_changed", {"id": "one"}, object()
        )

        self.assertFalse(allowed)
        self.assertEqual(
            namespace["statuses"][-1]["example_runtime_filter_last_error"],
            "provider unavailable",
        )


if __name__ == "__main__":
    unittest.main()
