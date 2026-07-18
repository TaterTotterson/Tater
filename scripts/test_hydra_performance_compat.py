import asyncio
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import helpers
from hydra import hydra_prompts
from hydra import hydra_ledger


def _local_result(text: str = "ok"):
    return {
        "model": "test-model",
        "message": {"role": "assistant", "content": text},
        "_usage": {
            "prompt_tokens": 2,
            "completion_tokens": 1,
            "total_tokens": 3,
        },
        "_timing": {
            "prompt_elapsed": 0.01,
            "completion_elapsed": 0.01,
            "speed_basis": "test",
        },
    }


class LlamaCppPerformanceTests(unittest.TestCase):
    def test_unified_vision_server_keeps_text_batch_settings(self):
        def n_ctx(*, vision=False):
            return 4096 if vision else 70000

        def n_batch(*, vision=False):
            return 128 if vision else 512

        def n_ubatch(*, vision=False):
            return 128 if vision else 0

        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch.object(helpers, "_llama_cpp_n_ctx", side_effect=n_ctx),
                mock.patch.object(helpers, "_llama_cpp_n_batch", side_effect=n_batch),
                mock.patch.object(helpers, "_llama_cpp_n_ubatch", side_effect=n_ubatch),
                mock.patch.object(helpers, "_llama_cpp_cache_reuse_tokens", return_value=256),
                mock.patch.object(helpers, "_llama_cpp_n_gpu_layers", return_value=-1),
                mock.patch.object(helpers, "_llama_cpp_slot_count", return_value=2),
                mock.patch.object(helpers, "_llama_cpp_slot_id", return_value=0),
                mock.patch.object(helpers, "_llama_cpp_mtp_enabled", return_value=False),
                mock.patch.object(helpers, "_llama_cpp_mtp_draft_model", return_value=""),
                mock.patch.object(helpers, "_llama_cpp_flash_attn_enabled", return_value=True),
                mock.patch.object(helpers, "_llama_cpp_offload_kqv_enabled", return_value=True),
                mock.patch.object(helpers, "_llama_cpp_chat_template_override_text", return_value=""),
                mock.patch.dict(
                    helpers.os.environ,
                    {
                        "TATER_LLAMA_CPP_CHAT_FORMAT": "",
                        "TATER_LLAMA_CPP_USE_MLOCK": "0",
                    },
                    clear=False,
                ),
            ):
                command, metadata = helpers._llama_cpp_native_server_command(
                    server_bin="/tmp/llama-server",
                    model_path="/tmp/model.gguf",
                    mmproj_path="/tmp/mmproj.gguf",
                    temp_dir=temp_dir,
                )

        self.assertEqual(command[command.index("--batch-size") + 1], "512")
        self.assertNotIn("--ubatch-size", command)
        self.assertEqual(command[command.index("--cache-reuse") + 1], "256")
        self.assertEqual(metadata["n_batch"], 512)
        self.assertEqual(metadata["cache_reuse_tokens"], 256)

    def test_cache_namespaces_get_stable_role_slots(self):
        def slot_id(scope="base", value=None):
            if value is not None:
                try:
                    return int(value)
                except Exception:
                    return -1
            return 1 if scope == "vision" else 0

        with (
            mock.patch.object(helpers, "_llama_cpp_slot_count", return_value=2),
            mock.patch.object(helpers, "_llama_cpp_slot_id", side_effect=slot_id),
        ):
            self.assertEqual(
                helpers._llama_cpp_cache_namespace_slot(
                    "hydra:astraeus", configured_slot=0
                ),
                0,
            )
            self.assertEqual(
                helpers._llama_cpp_cache_namespace_slot(
                    "hydra:hermes:final", configured_slot=0
                ),
                1,
            )
            self.assertEqual(
                helpers._llama_cpp_cache_namespace_slot(
                    "hydra:thanatos:state", configured_slot=0
                ),
                0,
            )
            self.assertEqual(
                helpers._llama_cpp_cache_namespace_slot(
                    "", configured_slot=0
                ),
                0,
            )
            self.assertEqual(
                helpers._llama_cpp_cache_namespace_slot(
                    "hydra:hermes:final", configured_slot=1, vision=True
                ),
                1,
            )

    def test_parent_engine_does_not_serialize_parallel_slots(self):
        class Engine:
            def __init__(self):
                self.active = 0
                self.max_active = 0
                self.lock = threading.Lock()

            def request(self, *_args, **_kwargs):
                with self.lock:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                time.sleep(0.08)
                with self.lock:
                    self.active -= 1
                return {"message": {"role": "assistant", "content": "ok"}}

        engine = Engine()
        bundle = {"engine": engine, "lock": threading.RLock()}
        with (
            mock.patch.object(
                helpers, "_load_llama_cpp_engine_bundle", return_value=bundle
            ),
            mock.patch.object(helpers, "_llama_cpp_slot_id", return_value=0),
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(
                        helpers._llama_cpp_engine_chat_completion,
                        "model",
                        [{"role": "user", "content": "hello"}],
                        {},
                        slot_id=index,
                    )
                    for index in range(2)
                ]
                [future.result(timeout=2) for future in futures]

        self.assertEqual(engine.max_active, 2)

    def test_native_stream_parser_emits_chunks_and_keeps_timings(self):
        class Response:
            status_code = 200
            text = ""

            def __init__(self):
                self.closed = False

            def raise_for_status(self):
                return None

            def iter_lines(self, decode_unicode=False):
                self.decode_unicode = decode_unicode
                return iter(
                    [
                        'data: {"content":"Spud Lord! 👋"}'.encode("utf-8"),
                        'data: {"content":" What’s good? 🐶🎮"}'.encode("utf-8"),
                        (
                            'data: {"content":"","timings":'
                            '{"prompt_n":2,"predicted_n":2}}'
                        ).encode("utf-8"),
                        b"data: [DONE]",
                    ]
                )

            def close(self):
                self.closed = True

        response = Response()
        session = SimpleNamespace(post=lambda *_args, **_kwargs: response)
        chunks = []
        with mock.patch.object(
            helpers, "_llama_cpp_native_http_session", return_value=session
        ):
            payload = helpers._llama_cpp_native_stream_post(
                "http://127.0.0.1:1234",
                "/completion",
                {"prompt": "x"},
                stream_callback=chunks.append,
            )

        self.assertFalse(response.decode_unicode)
        self.assertEqual(chunks, ["Spud Lord! 👋", " What’s good? 🐶🎮"])
        self.assertEqual(payload["content"], "Spud Lord! 👋 What’s good? 🐶🎮")
        self.assertEqual(payload["timings"]["prompt_n"], 2)
        self.assertTrue(response.closed)


class ProviderCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_llama_streams_without_forwarding_cache_metadata(self):
        client = helpers.LlamaCppLLMClientWrapper(model="test-model")
        captured = {}

        def fake_engine(
            model,
            messages,
            chat_kwargs,
            *,
            timeout=None,
            vision=False,
            slot_id=None,
            stream_callback=None,
        ):
            captured.update(
                {
                    "model": model,
                    "chat_kwargs": dict(chat_kwargs),
                    "slot_id": slot_id,
                    "vision": vision,
                }
            )
            stream_callback("o")
            stream_callback("k")
            return _local_result()

        chunks = []
        with (
            mock.patch.object(
                helpers, "_llama_cpp_engine_chat_completion", side_effect=fake_engine
            ),
            mock.patch.object(helpers, "_llama_cpp_slot_count", return_value=2),
            mock.patch.object(helpers, "_llama_cpp_slot_id", return_value=0),
        ):
            result = await client.chat(
                [{"role": "user", "content": "hello"}],
                cache_namespace="hydra:hermes:final",
                activity="chat",
                stream_callback=chunks.append,
            )

        self.assertEqual(result["message"]["content"], "ok")
        self.assertEqual(chunks, ["o", "k"])
        self.assertEqual(captured["slot_id"], 1)
        self.assertNotIn("cache_namespace", captured["chat_kwargs"])

    async def test_remote_llama_accepts_stream_hook_without_leaking_metadata(self):
        client = helpers.LlamaCppRemoteLLMClientWrapper(
            host="http://127.0.0.1:1234",
            model="test-model",
        )
        captured = {}

        def fake_chat(messages, *, timeout=None, **kwargs):
            captured.update(kwargs)
            kwargs["_stream_callback"]("ok")
            return _local_result()

        chunks = []
        with mock.patch.object(client, "_chat_sync", side_effect=fake_chat):
            result = await client.chat(
                [{"role": "user", "content": "hello"}],
                cache_namespace="hydra:hermes:final",
                activity="chat",
                stream_callback=chunks.append,
            )

        self.assertEqual(result["message"]["content"], "ok")
        self.assertEqual(chunks, ["ok"])
        self.assertEqual(captured["_cache_namespace"], "hydra:hermes:final")
        self.assertNotIn("cache_namespace", captured)

    async def test_transformers_consumes_cache_metadata_and_stream_falls_back(self):
        client = helpers.TransformersLLMClientWrapper(model="test-model")
        captured = {}

        def fake_chat(messages, *, timeout=None, **kwargs):
            captured.update(kwargs)
            return _local_result()

        chunks = []
        with mock.patch.object(client, "_chat_sync", side_effect=fake_chat):
            result = await client.chat(
                [{"role": "user", "content": "hello"}],
                cache_namespace="hydra:hermes:chat",
                activity="chat",
                stream_callback=chunks.append,
            )

        self.assertEqual(result["message"]["content"], "ok")
        self.assertNotIn("cache_namespace", captured)
        self.assertNotIn("activity", captured)
        self.assertEqual(chunks, ["ok"])

    async def test_mlx_consumes_cache_metadata_and_stream_falls_back(self):
        client = helpers.MlxLmLLMClientWrapper(model="test-model")
        captured = {}

        def fake_chat(messages, *, timeout=None, **kwargs):
            captured.update(kwargs)
            return _local_result()

        chunks = []
        with mock.patch.object(client, "_chat_sync", side_effect=fake_chat):
            result = await client.chat(
                [{"role": "user", "content": "hello"}],
                cache_namespace="hydra:hermes:final",
                activity="chat",
                stream_callback=chunks.append,
            )

        self.assertEqual(result["message"]["content"], "ok")
        self.assertNotIn("cache_namespace", captured)
        self.assertNotIn("activity", captured)
        self.assertEqual(chunks, ["ok"])

    async def test_openai_compatible_consumes_cache_metadata(self):
        class Completions:
            def __init__(self):
                self.kwargs = {}

            async def create(self, **kwargs):
                self.kwargs = dict(kwargs)
                message = SimpleNamespace(role="assistant", content="ok")
                return SimpleNamespace(
                    model="test-model",
                    choices=[SimpleNamespace(message=message)],
                    usage=SimpleNamespace(
                        prompt_tokens=2,
                        completion_tokens=1,
                        total_tokens=3,
                    ),
                )

        completions = Completions()
        wrapper = helpers.LLMClientWrapper(
            host="http://127.0.0.1:1234",
            model="test-model",
        )
        wrapper.client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )
        result = await wrapper.chat(
            [{"role": "user", "content": "hello"}],
            cache_namespace="hydra:astraeus",
            activity="planning",
        )

        self.assertEqual(result["message"]["content"], "ok")
        self.assertNotIn("cache_namespace", completions.kwargs)
        self.assertNotIn("activity", completions.kwargs)

    async def test_openai_compatible_native_stream_callback(self):
        class Stream:
            def __aiter__(self):
                async def events():
                    for text in ("o", "k"):
                        yield SimpleNamespace(
                            model="test-model",
                            choices=[
                                SimpleNamespace(
                                    delta=SimpleNamespace(content=text)
                                )
                            ],
                            usage=None,
                        )

                return events()

        class Completions:
            async def create(self, **kwargs):
                self.kwargs = dict(kwargs)
                return Stream()

        completions = Completions()
        wrapper = helpers.LLMClientWrapper(
            host="http://127.0.0.1:1234",
            model="test-model",
        )
        wrapper.client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )
        chunks = []
        result = await wrapper.chat(
            [{"role": "user", "content": "hello"}],
            stream_callback=chunks.append,
            cache_namespace="hydra:hermes:chat",
        )

        self.assertTrue(completions.kwargs["stream"])
        self.assertEqual(chunks, ["o", "k"])
        self.assertEqual(result["message"]["content"], "ok")

    async def test_openai_compatible_stream_rejection_falls_back(self):
        class Completions:
            def __init__(self):
                self.calls = []

            async def create(self, **kwargs):
                self.calls.append(dict(kwargs))
                if kwargs.get("stream"):
                    raise RuntimeError("streaming unsupported")
                return SimpleNamespace(
                    model="test-model",
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(
                                role="assistant",
                                content="ok",
                            )
                        )
                    ],
                    usage=SimpleNamespace(
                        prompt_tokens=2,
                        completion_tokens=1,
                        total_tokens=3,
                    ),
                )

        completions = Completions()
        wrapper = helpers.LLMClientWrapper(
            host="http://127.0.0.1:1234",
            model="test-model",
        )
        wrapper.client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )
        chunks = []
        result = await wrapper.chat(
            [{"role": "user", "content": "hello"}],
            stream_callback=chunks.append,
        )

        self.assertEqual(
            [call["stream"] for call in completions.calls],
            [True, False],
        )
        self.assertEqual(chunks, ["ok"])
        self.assertEqual(result["message"]["content"], "ok")

    async def test_spud_link_consumes_cache_metadata(self):
        class Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "model": "test-model",
                    "message": {"role": "assistant", "content": "ok"},
                    "usage": {
                        "prompt_tokens": 2,
                        "completion_tokens": 1,
                        "total_tokens": 3,
                    },
                }

        class Client:
            async def post(self, _url, *, json, headers, timeout):
                self.body = dict(json)
                self.headers = dict(headers)
                self.timeout = timeout
                return Response()

        client = helpers.SpudLinkLLMClientWrapper(
            host="http://127.0.0.1:1234",
            model="test-model",
            api_key="secret",
        )
        fake_http = Client()
        client._client = fake_http
        client._owns_http_client = False
        chunks = []
        result = await client.chat(
            [{"role": "user", "content": "hello"}],
            cache_namespace="hydra:hermes:chat",
            activity="chat",
            stream_callback=chunks.append,
        )

        self.assertEqual(result["message"]["content"], "ok")
        self.assertEqual(chunks, ["ok"])
        self.assertNotIn("cache_namespace", fake_http.body)
        self.assertNotIn("activity", fake_http.body)

    async def test_round_robin_is_role_affine_but_plain_calls_still_rotate(self):
        class Client:
            def __init__(self, name):
                self.name = name
                self.host = name
                self.model = name
                self.calls = 0

            async def chat(self, _messages, **_kwargs):
                self.calls += 1
                return {"message": {"role": "assistant", "content": self.name}}

        clients = [Client("a"), Client("b")]
        pool = helpers.RoundRobinLLMClientWrapper(
            clients=clients,
            pool_key="test-pool",
        )
        first = await pool.chat([], cache_namespace="hydra:hermes")
        second = await pool.chat([], cache_namespace="hydra:hermes")
        self.assertEqual(
            first["message"]["content"],
            second["message"]["content"],
        )

        plain_first = await pool.chat([])
        plain_second = await pool.chat([])
        self.assertNotEqual(
            plain_first["message"]["content"],
            plain_second["message"]["content"],
        )


class PromptCacheLayoutTests(unittest.TestCase):
    def test_volatile_time_is_after_stable_chat_instructions(self):
        marker = "Friday, July 17, 2026 at 10:30 PM"
        prompt = hydra_prompts.chat_fallback_system_prompt(
            platform="webui",
            platform_label="Web UI",
            now_text=marker,
            first_name="Tater",
            last_name="Totterson",
            personality="friendly",
            ascii_only_platforms=(),
        )
        self.assertGreater(prompt.index(marker), prompt.index("Reply naturally"))

    def test_volatile_time_is_after_stable_execution_instructions(self):
        marker = "Friday, July 17, 2026 at 10:30 PM"
        prompt = hydra_prompts.thanatos_system_prompt(
            platform="webui",
            now_text=marker,
            ascii_only_platforms=(),
        )
        self.assertGreater(prompt.index(marker), prompt.index("Execution role"))


class TelemetryTests(unittest.TestCase):
    def test_ledger_keeps_parallel_stage_timings_separate(self):
        class Redis:
            def __init__(self):
                self.rows = []

            def rpush(self, _key, payload):
                self.rows.append(payload)

            def ltrim(self, *_args):
                return None

        import json

        redis = Redis()
        hydra_ledger.write_hydra_ledger(
            redis_client=redis,
            platform="webui",
            scope="session:test",
            turn_id="turn",
            llm="test",
            user_message="hello",
            planned_tool=None,
            validation_status={},
            tool_result=None,
            checker_action="FINAL_ANSWER",
            planner_ms=100,
            astraeus_route_ms=80,
            thanatos_ms=200,
            progress_ms=30,
            state_update_ms=40,
            tool_ms=300,
            compact_tool_ref_fn=lambda _value: None,
            validation_status_for_ledger_fn=lambda **_kwargs: {},
            short_text_fn=lambda value, limit=0: str(value or "")[:limit or None],
            compact_agent_state_json_fn=lambda *_args, **_kwargs: "{}",
            agent_state_hash_fn=lambda *_args, **_kwargs: "",
            configured_max_ledger_items_fn=lambda _redis: 10,
            schema_version="2",
            agent_state_ledger_max_chars=100,
            allowed_planner_kinds=("answer",),
        )
        row = json.loads(redis.rows[0])
        self.assertEqual(row["planner_ms"], 100)
        self.assertEqual(row["thanatos_ms"], 200)
        self.assertEqual(row["progress_ms"], 30)
        self.assertEqual(row["state_update_ms"], 40)


class SpudLinkStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def _collect_events(self, callback_chunks):
        import tateros_app

        async def fake_completion(*_args, **kwargs):
            callback = kwargs.get("response_callback")
            if callable(callback):
                for chunk in callback_chunks:
                    callback(chunk)
            return {"content": "".join(callback_chunks), "artifacts": []}

        with (
            mock.patch.object(
                tateros_app,
                "_run_spud_link_native_hydra_completion",
                side_effect=fake_completion,
            ),
            mock.patch.object(tateros_app, "_save_little_spud_active_run"),
            mock.patch.object(tateros_app, "_save_little_spud_history"),
            mock.patch.object(tateros_app, "_clear_little_spud_active_run"),
            mock.patch.object(
                tateros_app,
                "_spud_link_follow_up_decision",
                new=mock.AsyncMock(return_value={}),
            ),
        ):
            stream = tateros_app._stream_spud_link_tater_completion(
                payload=SimpleNamespace(),
                messages=[{"role": "user", "content": "hello"}],
                tools_enabled=True,
                request=SimpleNamespace(),
                platform="little_spud",
                origin_override={},
                scope_override="little-spud:test",
                platform_preamble="Little Spud test.",
                context_extra={},
                little_spud_identity={"scope": "little-spud:test"},
            )
            return [event async for event in stream]

    async def test_spud_link_emits_real_chunks_before_final_message(self):
        events = await self._collect_events(["hel", "lo"])

        chunk_index = next(
            index
            for index, event in enumerate(events)
            if "event: tater.response_chunk" in event
        )
        message_index = next(
            index
            for index, event in enumerate(events)
            if "event: tater.message" in event
        )

        self.assertLess(chunk_index, message_index)
        self.assertIn('"chunk": "hello"', events[chunk_index])

    async def test_spud_link_keeps_one_shot_provider_reply_atomic(self):
        events = await self._collect_events(["complete response"])

        self.assertFalse(
            any("event: tater.response_chunk" in event for event in events)
        )
        self.assertTrue(any("event: tater.message" in event for event in events))


class ChatRuntimeReuseTests(unittest.TestCase):
    def test_chat_jobs_share_one_async_runtime_and_emit_stream_chunks(self):
        import tateros_app

        manager = tateros_app.ChatJobManager()
        loop_ids = []

        async def fake_process_message(**kwargs):
            loop_ids.append(id(asyncio.get_running_loop()))
            callback = kwargs.get("response_callback")
            if callable(callback):
                callback("hel")
                callback("lo")
            return {
                "responses": ["hello"],
                "agent": True,
                "task_name": "test",
            }

        try:
            with (
                mock.patch.object(
                    tateros_app,
                    "_process_message",
                    side_effect=fake_process_message,
                ),
                mock.patch.object(tateros_app, "_save_chat_message"),
            ):
                job_ids = []
                for index in range(2):
                    created = manager.create_job(
                        user_name="User",
                        message=f"message {index}",
                        session_id="session",
                    )
                    job_id = created["job_id"]
                    job_ids.append(job_id)
                    with manager.lock:
                        future = manager.jobs[job_id]["future"]
                    future.result(timeout=3)

                with manager.lock:
                    event_types = []
                    for job_id in job_ids:
                        event_queue = manager.jobs[job_id]["events"]
                        while not event_queue.empty():
                            event_types.append(event_queue.get_nowait()["type"])

            self.assertEqual(len(set(loop_ids)), 1)
            self.assertIn("response_chunk", event_types)
            self.assertIn("done", event_types)
        finally:
            manager.shutdown(timeout=2)

    def test_chat_jobs_keep_one_shot_provider_reply_atomic(self):
        import tateros_app

        manager = tateros_app.ChatJobManager()

        async def fake_process_message(**kwargs):
            callback = kwargs.get("response_callback")
            if callable(callback):
                callback("complete response")
            return {
                "responses": ["complete response"],
                "agent": True,
                "task_name": "test",
            }

        try:
            with (
                mock.patch.object(
                    tateros_app,
                    "_process_message",
                    side_effect=fake_process_message,
                ),
                mock.patch.object(tateros_app, "_save_chat_message"),
            ):
                created = manager.create_job(
                    user_name="User",
                    message="message",
                    session_id="session",
                )
                job_id = created["job_id"]
                with manager.lock:
                    future = manager.jobs[job_id]["future"]
                future.result(timeout=3)

                with manager.lock:
                    event_queue = manager.jobs[job_id]["events"]
                    event_types = []
                    while not event_queue.empty():
                        event_types.append(event_queue.get_nowait()["type"])

            self.assertNotIn("response_chunk", event_types)
            self.assertIn("done", event_types)
        finally:
            manager.shutdown(timeout=2)


if __name__ == "__main__":
    unittest.main()
