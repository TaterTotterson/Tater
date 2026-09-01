#!/usr/bin/env python3
from __future__ import annotations

import json
import platform
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from spudex import chat_loop, hydra_tools, policy, runner, settings  # noqa: E402


class _FakeRedis:
    def __init__(self, values: dict[str, object]) -> None:
        self.values = settings.normalize_spudex_settings(values)

    def get(self, key: str) -> str | None:
        if key == settings.SPUDEX_SETTINGS_KEY:
            return json.dumps(self.values)
        return None


class _SequenceLlm:
    def __init__(self, *outputs: str) -> None:
        self.outputs = list(outputs)
        self.calls: list[dict[str, object]] = []

    async def chat(self, **kwargs):
        self.calls.append(dict(kwargs))
        output = self.outputs.pop(0) if self.outputs else ""
        return {"message": {"role": "assistant", "content": output}}


class SpudexRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.host_temp_dir = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp_dir.name)
        self.host_root = pathlib.Path(self.host_temp_dir.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.patchers = [
            mock.patch.object(runner, "AGENT_LAB_DIR", self.root),
            mock.patch.object(runner, "SPUDEX_DIR", self.root / "spudex"),
            mock.patch.object(runner, "SESSIONS_DIR", self.root / "spudex" / "sessions"),
            mock.patch.object(policy, "AGENT_LAB_DIR", self.root),
            mock.patch.object(policy, "AGENT_WORKSPACE_DIR", self.workspace),
        ]
        for patcher in self.patchers:
            patcher.start()
        runner._ACTIVE_PROCESSES.clear()
        runner._ACTIVE_TASKS.clear()

    async def asyncTearDown(self) -> None:
        await runner.shutdown_spudex_runtime()

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.host_temp_dir.cleanup()
        self.temp_dir.cleanup()

    @staticmethod
    def _redis(**overrides: object) -> _FakeRedis:
        return _FakeRedis(
            {
                "policy_enabled": False,
                "require_approval": False,
                "max_task_steps": 3,
                "command_timeout_sec": 10,
                **overrides,
            }
        )

    def _session(self, label: str = "test") -> str:
        session = runner.create_spudex_session(
            label=label,
            cwd=str(self.workspace),
            goal=label,
            source="test",
            platform="webui",
        )
        return str(session["id"])

    def test_settings_apply_sane_execution_caps(self) -> None:
        normalized = settings.normalize_spudex_settings(
            {
                "max_task_steps": 1000000,
                "max_output_bytes": 1,
                "max_concurrent_processes": 1000,
            }
        )

        self.assertEqual(normalized["max_task_steps"], 50)
        self.assertEqual(normalized["max_output_bytes"], 16384)
        self.assertEqual(normalized["max_concurrent_processes"], 16)

    def test_legacy_workspace_default_migrates_to_agent_lab_home(self) -> None:
        normalized = settings.normalize_spudex_settings(
            {"sandbox_mode": "agent_lab", "default_cwd": "workspace"}
        )

        self.assertEqual(normalized["filesystem_scope"], "host")
        self.assertEqual(normalized["default_cwd"], "agent_lab")

    def test_subprocess_environment_does_not_inherit_secrets(self) -> None:
        with mock.patch.dict(
            runner.os.environ,
            {
                "PATH": "/usr/bin:/bin",
                "OPENAI_API_KEY": "do-not-copy",
                "TATER_SECRET": "do-not-copy",
            },
            clear=True,
        ):
            env = runner._subprocess_env()

        self.assertNotIn("OPENAI_API_KEY", env)
        self.assertNotIn("TATER_SECRET", env)
        self.assertEqual(env["HOME"], str(self.root))
        self.assertEqual(env["SPUDEX_AGENT_LAB"], str(self.root))

    def test_policy_allows_host_paths(self) -> None:
        absolute = policy.validate_spudex_command(
            ["tool", "--output=/tmp/outside.txt"],
            self.workspace,
            settings.normalize_spudex_settings(),
        )
        allowed = policy.validate_spudex_command(
            ["tool", "--output=inside/result.txt"],
            self.workspace,
            settings.normalize_spudex_settings(),
        )

        self.assertTrue(absolute["ok"])
        self.assertTrue(allowed["ok"])

    def test_empty_api_argv_falls_back_to_manual_command_text(self) -> None:
        self.assertEqual(policy.normalize_argv(command="ls -la", argv=[]), ["ls", "-la"])

    def test_macos_isolation_blocks_network_unless_enabled(self) -> None:
        base_settings = settings.normalize_spudex_settings(
            {"policy_enabled": True, "allow_network": False}
        )
        with (
            mock.patch.object(runner.host_platform, "system", return_value="Darwin"),
            mock.patch.object(runner.shutil, "which", return_value="/usr/bin/sandbox-exec"),
        ):
            blocked_argv, blocked_backend = runner._isolated_exec_argv(
                ["python3", "script.py"],
                cwd=self.workspace,
                settings=base_settings,
            )
            allowed_argv, _allowed_backend = runner._isolated_exec_argv(
                ["python3", "script.py"],
                cwd=self.workspace,
                settings={**base_settings, "allow_network": True},
            )

        self.assertEqual(blocked_backend, "macos_sandbox")
        self.assertEqual(blocked_argv[0], "/usr/bin/sandbox-exec")
        self.assertIn("(allow file-write*)", blocked_argv[2])
        self.assertNotIn("subpath", blocked_argv[2])
        self.assertNotIn("(allow network*)", blocked_argv[2])
        self.assertIn("(allow network*)", allowed_argv[2])

    async def test_command_capture_is_bounded(self) -> None:
        script = self.workspace / "large_output.py"
        script.write_text("print('x' * 50000)\n", encoding="utf-8")
        session_id = self._session("bounded output")
        runtime_settings = settings.normalize_spudex_settings(
            {
                "policy_enabled": False,
                "max_output_bytes": 16384,
                "command_timeout_sec": 10,
            }
        )

        result = await runner.run_argv_in_session(
            session_id,
            argv=[sys.executable, str(script)],
            cwd=self.workspace,
            settings=runtime_settings,
            capture_output=True,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["output_truncated"])
        self.assertLessEqual(len(result["stdout"].encode("utf-8")), 16384)

    async def test_manual_terminal_starts_in_agent_lab_home(self) -> None:
        result = await runner.start_spudex_command(
            command="pwd",
            cwd="",
            source="ui",
            redis_client=self._redis(policy_enabled=True),
            label="pwd",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["builtin"], "pwd")
        self.assertEqual(pathlib.Path(result["session"]["cwd"]).resolve(), self.root.resolve())
        self.assertEqual(result["session"]["cwd_display"], "~")
        logs = runner.read_spudex_logs(result["session"]["id"])["entries"]
        self.assertTrue(any(row["stream"] == "stdout" and row["text"] == str(self.root.resolve()) for row in logs))

    async def test_manual_terminal_can_change_directories_inside_agent_lab(self) -> None:
        result = await runner.start_spudex_command(
            command="cd ..",
            cwd=str(self.workspace),
            source="ui",
            redis_client=self._redis(policy_enabled=True),
            label="cd ..",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["builtin"], "cd")
        self.assertEqual(pathlib.Path(result["session"]["cwd"]).resolve(), self.root.resolve())
        self.assertEqual(result["session"]["cwd_display"], "~")
        self.assertEqual(result["session"]["status"], "succeeded")
        logs = runner.read_spudex_logs(result["session"]["id"])["entries"]
        self.assertTrue(any(row["stream"] == "stdout" and row["text"] == "~" for row in logs))

    async def test_manual_terminal_can_leave_agent_lab(self) -> None:
        result = await runner.start_spudex_command(
            command=f"cd {self.host_root}",
            cwd=str(self.root),
            source="ui",
            redis_client=self._redis(policy_enabled=True),
            label="cd host path",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["session"]["status"], "succeeded")
        self.assertEqual(pathlib.Path(result["session"]["cwd"]).resolve(), self.host_root.resolve())
        self.assertEqual(result["session"]["cwd_display"], str(self.host_root.resolve()))

    async def test_ls_and_dir_builtins_browse_host_paths_without_a_subprocess(self) -> None:
        (self.host_root / "visible.txt").write_text("hello\n", encoding="utf-8")
        with mock.patch.object(
            runner.asyncio,
            "create_subprocess_exec",
            new_callable=mock.AsyncMock,
            side_effect=AssertionError("terminal builtin launched a subprocess"),
        ):
            ls_result = await runner.start_spudex_command(
                command=f"ls -la {self.host_root}",
                argv=[],
                cwd=str(self.root),
                source="ui",
                redis_client=self._redis(policy_enabled=True),
                label="ls host path",
            )
            dir_result = await runner.start_spudex_command(
                command=f"dir {self.host_root}",
                cwd=str(self.root),
                source="ui",
                redis_client=self._redis(policy_enabled=True),
                label="dir host path",
            )

        self.assertTrue(ls_result["ok"])
        self.assertTrue(dir_result["ok"])
        self.assertEqual(ls_result["isolation_backend"], "terminal_builtin")
        self.assertEqual(dir_result["isolation_backend"], "terminal_builtin")
        for result in (ls_result, dir_result):
            logs = runner.read_spudex_logs(result["session"]["id"])["entries"]
            self.assertTrue(any("visible.txt" in row["text"] for row in logs))

    async def test_spudex_file_writer_accepts_absolute_host_path(self) -> None:
        session_id = self._session("host write")
        target = self.host_root / "written.txt"

        result = runner.write_spudex_file_in_session(
            session_id,
            path=str(target),
            content="outside agent_lab\n",
            cwd=self.root,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(target.read_text(encoding="utf-8"), "outside agent_lab\n")
        self.assertEqual(result["path_display"], str(target.resolve()))

    async def test_cd_without_a_path_returns_to_agent_lab_home(self) -> None:
        result = await runner.start_spudex_command(
            command="cd",
            cwd=str(self.host_root),
            source="ui",
            redis_client=self._redis(policy_enabled=True),
            label="cd home",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(pathlib.Path(result["session"]["cwd"]).resolve(), self.root.resolve())
        self.assertEqual(result["session"]["cwd_display"], "~")

    async def test_manual_terminal_rejects_a_missing_directory(self) -> None:
        result = await runner.start_spudex_command(
            command=f"cd {self.host_root / 'missing'}",
            cwd=str(self.root),
            source="ui",
            redis_client=self._redis(policy_enabled=True),
            label="cd missing",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["session"]["status"], "failed")
        logs = runner.read_spudex_logs(result["session"]["id"])["entries"]
        self.assertTrue(any("Directory does not exist" in row["text"] for row in logs))

    @unittest.skipUnless(platform.system() == "Darwin", "macOS sandbox integration")
    async def test_macos_policy_runner_uses_os_network_sandbox(self) -> None:
        session_id = self._session("macOS isolation")
        runtime_settings = settings.normalize_spudex_settings(
            {
                "policy_enabled": True,
                "allow_network": False,
                "command_timeout_sec": 10,
            }
        )

        target = self.host_root / "sandbox-write.txt"
        result = await runner.run_argv_in_session(
            session_id,
            argv=["touch", str(target)],
            cwd=self.workspace,
            settings=runtime_settings,
            capture_output=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["isolation_backend"], "macos_sandbox")
        self.assertTrue(target.exists())

    async def test_shutdown_stops_background_processes(self) -> None:
        script = self.workspace / "sleep.py"
        script.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
        session_id = self._session("background process")
        runtime_settings = settings.normalize_spudex_settings(
            {
                "policy_enabled": False,
                "command_timeout_sec": 120,
            }
        )
        started = await runner.run_argv_in_session(
            session_id,
            argv=[sys.executable, str(script)],
            cwd=self.workspace,
            settings=runtime_settings,
            background=True,
        )

        self.assertTrue(started["ok"])
        self.assertIn(session_id, runner._ACTIVE_PROCESSES)
        stopped = await runner.shutdown_spudex_runtime()

        self.assertGreaterEqual(stopped["processes_stopped"], 1)
        self.assertNotIn(session_id, runner._ACTIVE_PROCESSES)
        self.assertEqual(runner.get_spudex_session(session_id)["status"], "stopped")

    async def test_invalid_json_gets_one_repair_attempt(self) -> None:
        llm = _SequenceLlm(
            "not valid json",
            '{"type":"reply","outcome":"answer","message":"The host is ready."}',
        )
        session_id = self._session("repair")

        result = await chat_loop.run_spudex_chat_turn(
            session_id=session_id,
            message="Is the host ready?",
            platform="webui",
            llm_client=llm,
            redis_client=self._redis(max_task_steps=1),
            task_mode=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(llm.calls), 2)

    async def test_unrepairable_json_fails_truthfully(self) -> None:
        llm = _SequenceLlm("bad", "still bad")
        session_id = self._session("bad json")

        result = await chat_loop.run_spudex_chat_turn(
            session_id=session_id,
            message="Run a task",
            platform="webui",
            llm_client=llm,
            redis_client=self._redis(max_task_steps=1),
            task_mode=True,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "spudex_invalid_model_json")
        self.assertEqual(runner.get_spudex_session(session_id)["status"], "failed")

    async def test_step_limit_is_incomplete_not_success(self) -> None:
        llm = _SequenceLlm(
            '{"type":"command","argv":["pwd"],"reason":"Inspect the working folder."}'
        )
        session_id = self._session("step limit")

        result = await chat_loop.run_spudex_chat_turn(
            session_id=session_id,
            message="Inspect and report the working folder",
            platform="webui",
            llm_client=llm,
            redis_client=self._redis(max_task_steps=1),
            task_mode=True,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "spudex_step_limit")
        self.assertEqual(runner.get_spudex_session(session_id)["status"], "incomplete")

    async def test_task_mode_rejects_unearned_completion(self) -> None:
        llm = _SequenceLlm(
            '{"type":"reply","outcome":"completed","message":"Everything is done."}'
        )
        session_id = self._session("unearned completion")

        result = await chat_loop.run_spudex_chat_turn(
            session_id=session_id,
            message="Create the requested file",
            platform="webui",
            llm_client=llm,
            redis_client=self._redis(max_task_steps=1),
            task_mode=True,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "spudex_task_failed")

    async def test_spudex_streams_action_and_completion_progress(self) -> None:
        llm = _SequenceLlm(
            '{"type":"command","argv":["pwd"],"reason":"Inspect the working folder."}',
            '{"type":"reply","outcome":"completed","message":"The folder was inspected."}',
        )
        session_id = self._session("progress")
        progress: list[dict[str, object]] = []

        async def on_progress(_func, _plugin, _text, payload):
            progress.append(dict(payload))

        result = await chat_loop.run_spudex_chat_turn(
            session_id=session_id,
            message="Inspect the working folder",
            platform="little_spud",
            llm_client=llm,
            redis_client=self._redis(max_task_steps=2),
            task_mode=True,
            progress_callback=on_progress,
        )

        phases = [str(row.get("phase") or "") for row in progress]
        self.assertTrue(result["ok"])
        self.assertIn("spudex_action", phases)
        self.assertIn("spudex_step_result", phases)
        self.assertIn("spudex_completed", phases)

    async def test_hydra_uses_the_shared_spudex_loop(self) -> None:
        expected = {"ok": True, "summary_for_user": "done"}
        with mock.patch.object(
            hydra_tools,
            "run_spudex_chat_turn",
            new_callable=mock.AsyncMock,
            return_value=expected,
        ) as shared_loop:
            result = await hydra_tools._run_spudex_task(
                args={"request": "Inspect the host"},
                platform="webui",
                llm_client=_SequenceLlm(),
                redis_client=self._redis(),
            )

        self.assertEqual(result, expected)
        self.assertTrue(shared_loop.await_args.kwargs["task_mode"])

    async def test_shared_hydra_loop_preserves_existing_approval_block(self) -> None:
        llm = _SequenceLlm(
            '{"type":"command","argv":["pwd"],"reason":"Inspect the folder."}'
        )

        result = await hydra_tools._run_spudex_task(
            args={"request": "Inspect the folder"},
            platform="webui",
            llm_client=llm,
            redis_client=self._redis(require_approval=True, max_task_steps=1),
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "spudex_approval_required")


if __name__ == "__main__":
    unittest.main()
