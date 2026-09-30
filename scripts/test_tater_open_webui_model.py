from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

import tateros_app


class FakeRedis:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True

    def delete(self, key):
        self.values.pop(key, None)
        return 1


def payload(model: str):
    return SimpleNamespace(
        model=model,
        messages=[{"role": "user", "content": "hello"}],
        metadata={},
        user=None,
        user_name=None,
    )


class TaterOpenWebUiModelTests(unittest.IsolatedAsyncioTestCase):
    def test_dedicated_route_resolves_complete_client_kwargs(self):
        prefix = tateros_app.TATER_OPEN_WEBUI_LLM_KEY_PREFIX
        redis = FakeRedis(
            {
                f"{prefix}provider": "openai_compatible",
                f"{prefix}host": "http://coding-model.local",
                f"{prefix}port": "8080",
                f"{prefix}model": "coder-large",
                f"{prefix}api_key": "secret",
            }
        )
        with mock.patch.object(tateros_app, "redis_client", redis):
            kwargs = tateros_app._tater_open_webui_llm_client_kwargs()

        self.assertEqual(kwargs["provider"], "openai_compatible")
        self.assertEqual(kwargs["host"], "http://coding-model.local:8080")
        self.assertEqual(kwargs["model"], "coder-large")
        self.assertEqual(kwargs["api_key"], "secret")

    def test_empty_override_falls_back_to_base(self):
        redis = FakeRedis()
        with mock.patch.object(tateros_app, "redis_client", redis):
            kwargs = tateros_app._tater_open_webui_llm_client_kwargs()
        self.assertEqual(kwargs, {"redis_conn": redis})

    async def test_linked_webui_base_call_receives_dedicated_route(self):
        request = SimpleNamespace(headers={})
        dedicated = {"provider": "llama_cpp", "model": "coder.gguf"}
        with (
            mock.patch.object(
                tateros_app,
                "_tater_open_webui_llm_client_kwargs",
                return_value=dedicated,
            ),
            mock.patch.object(
                tateros_app,
                "_run_tater_api_direct_completion",
                new=mock.AsyncMock(return_value={"ok": True}),
            ) as run_direct,
        ):
            result = await tateros_app._run_tater_api_chat_completion(
                payload("tater/base"),
                settings={
                    "mode": "direct",
                    "auth_mode": "spud_link",
                    "linked_node_id": "webui-node",
                },
                request=request,
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(run_direct.await_args.kwargs["llm_client_kwargs"], dedicated)

    async def test_hydra_call_does_not_read_webui_model_override(self):
        request = SimpleNamespace(headers={})
        with (
            mock.patch.object(tateros_app, "_tater_open_webui_llm_client_kwargs") as webui_route,
            mock.patch.object(
                tateros_app,
                "_run_tater_api_hydra_completion",
                new=mock.AsyncMock(return_value={"ok": True}),
            ) as run_hydra,
        ):
            result = await tateros_app._run_tater_api_chat_completion(
                payload("tater/hydra"),
                settings={"mode": "direct", "hydra_tools_enabled": True},
                request=request,
            )

        self.assertEqual(result, {"ok": True})
        run_hydra.assert_awaited_once()
        webui_route.assert_not_called()


if __name__ == "__main__":
    unittest.main()
