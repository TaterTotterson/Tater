from __future__ import annotations

import ast
import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import socket
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from fastapi import FastAPI, HTTPException, Request
import httpx
import uvicorn

import helpers
from tater.web.request_lifecycle import run_until_disconnected


class RequestLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def connected_request(self):
        async def receive():
            await asyncio.Future()

        return SimpleNamespace(receive=receive)

    async def test_completed_calls_return_without_a_disconnect(self):
        request = self.connected_request()
        result = await run_until_disconnected(request, asyncio.sleep(0, result={"ok": True}))
        self.assertEqual(result, {"ok": True})

    async def test_handler_shutdown_cancels_and_awaits_the_model_operation(self):
        started, closed = asyncio.Event(), asyncio.Event()

        async def generate():
            started.set()
            try:
                await asyncio.Future()
            finally:
                closed.set()

        task = asyncio.create_task(run_until_disconnected(self.connected_request(), generate()))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(closed.is_set())

    async def test_tcp_disconnect_stops_native_tool_generation_before_response_headers(self):
        """Real sockets through the API, scheduler, worker tool path and native client."""
        started, native_closed = threading.Event(), threading.Event()

        class NativeHandler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if body["messages"][0]["content"] == "Quick request":
                    response = json.dumps(
                        {
                            "choices": [
                                {
                                    "message": {"role": "assistant", "content": "Still responding"},
                                    "finish_reason": "stop",
                                }
                            ]
                        }
                    ).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                    return
                started.set()
                self.connection.settimeout(0.05)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    try:
                        if not self.connection.recv(1):
                            native_closed.set()
                            return
                    except socket.timeout:
                        continue

        native = ThreadingHTTPServer(("127.0.0.1", 0), NativeHandler)
        native.daemon_threads = True
        native_thread = threading.Thread(target=native.serve_forever, daemon=True)
        native_thread.start()
        base = f"http://127.0.0.1:{native.server_port}"
        events = []

        def engine(model, messages, chat_kwargs, *, cancel_event=None, **kwargs):
            events.append(cancel_event)
            return helpers._llama_cpp_native_worker_chat(
                {},
                model,
                messages,
                chat_kwargs,
                vision=False,
                cancel_event=cancel_event,
                timeout=30,
            )

        source = Path(__file__).resolve().parents[1] / "tater/web/application.py"
        node = next(
            node
            for node in ast.parse(source.read_text()).body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "tater_openai_chat_completions"
        )
        node.decorator_list = []
        module = ast.fix_missing_locations(
            ast.Module(
                body=[
                    ast.ImportFrom(
                        module="__future__", names=[ast.alias(name="annotations")], level=0
                    ),
                    node,
                ],
                type_ignores=[],
            )
        )
        scope = {
            "run_until_disconnected": run_until_disconnected,
            "_require_tater_api_request": lambda request: {},
        }

        async def complete(payload, *, settings, request):
            client = helpers.LlamaCppLLMClientWrapper(model="api-disconnect-test")
            return await client.chat(
                [{"role": "user", "content": payload.prompt}],
                tools=[
                    {"type": "function", "function": {"name": "dashboard_design", "parameters": {}}}
                ],
            )

        scope["_run_tater_api_chat_completion"] = complete
        exec(compile(module, str(source), "exec"), scope)
        app = FastAPI()

        @app.middleware("http")
        async def middleware(request, call_next):
            return await call_next(request)

        @app.post("/v1/chat/completions")
        async def endpoint(request: Request):
            body = await request.json()
            return await scope["tater_openai_chat_completions"](
                SimpleNamespace(stream=False, prompt=body.get("prompt", "Design a dashboard")),
                request,
            )

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        url = f"http://127.0.0.1:{sock.getsockname()[1]}/v1/chat/completions"
        server = uvicorn.Server(uvicorn.Config(app, log_level="critical", lifespan="off"))

        def serve():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(server.serve(sockets=[sock]))
            finally:
                loop.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            for _ in range(300):
                if server.started:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(server.started)
            with (
                mock.patch.object(helpers, "_llama_cpp_engine_chat_completion", side_effect=engine),
                mock.patch.object(
                    helpers, "_llama_cpp_native_worker_load", return_value={"slot_count": 5}
                ),
            ):
                # Native metadata is read from the supplied worker state.
                original = helpers._llama_cpp_native_worker_load

                def load(state, *args, **kwargs):
                    state["base_url"] = base
                    return original(state, *args, **kwargs)

                with mock.patch.object(helpers, "_llama_cpp_native_worker_load", side_effect=load):
                    async with httpx.AsyncClient() as client:
                        pending = asyncio.create_task(
                            client.post(url, json={"messages": []}, timeout=4)
                        )
                        self.assertTrue(await asyncio.to_thread(started.wait, 2))
                        pending.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await pending
                        for _ in range(100):
                            if events[0].is_set():
                                break
                            await asyncio.sleep(0.01)
                        self.assertTrue(
                            events[0].is_set(), "HTTP disconnect must reach the model task"
                        )
                        self.assertTrue(
                            await asyncio.to_thread(native_closed.wait, 2),
                            "native inference must lose its connection promptly",
                        )
                        self.assertTrue(events[0].is_set())
                        quick = await client.post(url, json={"prompt": "Quick request"}, timeout=2)
                        self.assertEqual(quick.status_code, 200)
                        self.assertEqual(quick.json()["message"]["content"], "Still responding")
                        self.assertFalse(
                            events[1].is_set(), "cancelling one request must not cancel others"
                        )
        finally:
            server.should_exit = True
            await asyncio.to_thread(thread.join, 3)
            sock.close()
            await asyncio.to_thread(native.shutdown)
            native.server_close()


if __name__ == "__main__":
    unittest.main()
