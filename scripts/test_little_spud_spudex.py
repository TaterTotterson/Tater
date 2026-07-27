#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from spudex import hydra_tools  # noqa: E402
import tateros_app  # noqa: E402
import tool_runtime  # noqa: E402


class _AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None


class LittleSpudSpudexTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _admin_status(*, matched: bool, is_admin: bool) -> dict[str, object]:
        return {
            "platform": "little_spud",
            "matched": matched,
            "person_id": "person:admin" if matched else "",
            "person_name": "Tater Admin" if matched else "",
            "is_admin": is_admin,
        }

    def test_unlinked_little_spud_does_not_advertise_spudex(self) -> None:
        with (
            mock.patch.object(hydra_tools, "spudex_enabled_for_platform", return_value=True),
            mock.patch.object(
                hydra_tools,
                "resolve_admin_status",
                return_value=self._admin_status(matched=False, is_admin=False),
            ),
        ):
            rows = hydra_tools.spudex_hydra_tool_rows(
                platform="little_spud",
                origin={"device_id": "little-spud-1"},
            )

        self.assertEqual(rows, [])

    def test_non_admin_little_spud_does_not_advertise_spudex(self) -> None:
        with (
            mock.patch.object(hydra_tools, "spudex_enabled_for_platform", return_value=True),
            mock.patch.object(
                hydra_tools,
                "resolve_admin_status",
                return_value=self._admin_status(matched=True, is_admin=False),
            ),
        ):
            rows = hydra_tools.spudex_hydra_tool_rows(
                platform="little_spud",
                origin={"person_id": "person:member"},
            )

        self.assertEqual(rows, [])

    def test_admin_little_spud_advertises_spudex(self) -> None:
        with (
            mock.patch.object(hydra_tools, "spudex_enabled_for_platform", return_value=True),
            mock.patch.object(
                hydra_tools,
                "resolve_admin_status",
                return_value=self._admin_status(matched=True, is_admin=True),
            ),
        ):
            rows = hydra_tools.spudex_hydra_tool_rows(
                platform="little_spud",
                origin={"person_id": "person:admin"},
            )

        self.assertEqual([row["id"] for row in rows], ["run_terminal_task"])

    async def test_non_admin_little_spud_cannot_execute_spudex(self) -> None:
        with (
            mock.patch.object(hydra_tools, "spudex_enabled_for_platform", return_value=True),
            mock.patch.object(
                hydra_tools,
                "resolve_admin_status",
                return_value=self._admin_status(matched=True, is_admin=False),
            ),
            mock.patch.object(
                hydra_tools,
                "_run_spudex_task",
                new_callable=mock.AsyncMock,
            ) as run_task,
        ):
            result = await hydra_tools.run_spudex_hydra_tool(
                tool_id="run_terminal_task",
                args={"request": "show the current directory"},
                platform="little_spud",
                origin={"person_id": "person:member"},
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "spudex_little_spud_admin_required")
        run_task.assert_not_awaited()

    async def test_admin_little_spud_can_reach_spudex_execution(self) -> None:
        expected = {"ok": True, "summary_for_user": "done"}
        with (
            mock.patch.object(hydra_tools, "spudex_enabled_for_platform", return_value=True),
            mock.patch.object(
                hydra_tools,
                "resolve_admin_status",
                return_value=self._admin_status(matched=True, is_admin=True),
            ),
            mock.patch.object(hydra_tools, "spudex_llm_overrides", return_value={}),
            mock.patch.object(
                hydra_tools,
                "_run_spudex_task",
                new_callable=mock.AsyncMock,
                return_value=expected,
            ) as run_task,
        ):
            result = await hydra_tools.run_spudex_hydra_tool(
                tool_id="run_terminal_task",
                args={"request": "show the current directory"},
                platform="little_spud",
                origin={"person_id": "person:admin"},
                llm_client=object(),
            )

        self.assertEqual(result, expected)
        run_task.assert_awaited_once()

    def test_spudex_approval_policy_is_preserved(self) -> None:
        blocked = hydra_tools._approval_required(
            {"require_approval": True},
            ["pwd"],
            actor="Little Spud",
        )

        self.assertIsNotNone(blocked)
        self.assertEqual(blocked["error"]["code"], "spudex_approval_required")
        self.assertIsNone(
            hydra_tools._approval_required(
                {"require_approval": False},
                ["pwd"],
                actor="Little Spud",
            )
        )

    def test_disabling_little_spud_tools_removes_all_kernel_tools(self) -> None:
        origin = {"kernel_tools_enabled": False}

        self.assertEqual(
            tool_runtime.kernel_tool_ids(platform="little_spud", origin=origin),
            [],
        )
        listed = tool_runtime.list_tools(
            platform="little_spud",
            registry={},
            origin=origin,
        )
        self.assertEqual(listed["kernel_tools"], [])

    async def test_disabling_little_spud_tools_blocks_direct_kernel_execution(self) -> None:
        result = await tool_runtime.run_meta_tool(
            func="read_file",
            args={"path": "/tmp/example"},
            platform="little_spud",
            registry={},
            origin={"kernel_tools_enabled": False},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "kernel_tools_disabled")

    async def test_tool_runtime_passes_platform_progress_to_spudex(self) -> None:
        progress_callback = mock.AsyncMock()
        with (
            mock.patch.object(tool_runtime, "_admin_gate_blocks_kernel", return_value=None),
            mock.patch.object(
                tool_runtime,
                "run_spudex_hydra_tool",
                new_callable=mock.AsyncMock,
                return_value={"ok": True, "summary_for_user": "done"},
            ) as run_spudex,
        ):
            result = await tool_runtime.run_meta_tool(
                func="run_terminal_task",
                args={"request": "inspect the host"},
                platform="little_spud",
                registry={},
                origin={"kernel_tools_enabled": True},
                progress_callback=progress_callback,
            )

        self.assertTrue(result["ok"])
        self.assertIs(
            run_spudex.await_args.kwargs["progress_callback"],
            progress_callback,
        )

    def test_spudex_platform_picker_includes_little_spud(self) -> None:
        with mock.patch.object(
            tateros_app,
            "_load_spud_link_settings",
            return_value={"mode": "hub", "allow_little_spuds": True},
        ):
            rows = tateros_app._spudex_platform_options(
                {"allowed_platforms": ["little_spud"]}
            )

        little_spud = next(row for row in rows if row["value"] == "little_spud")
        self.assertEqual(little_spud["label"], "Little Spud")
        self.assertTrue(little_spud["running"])
        self.assertTrue(little_spud["saved"])

    async def test_little_spud_tools_setting_reaches_hydra_origin(self) -> None:
        fake_client = object()
        request = mock.Mock()
        request.headers = {}
        payload = mock.Mock()
        payload.user = "little-spud-user"

        with (
            mock.patch.object(
                tateros_app,
                "_openai_user_text_and_history",
                return_value=("hello", []),
            ),
            mock.patch.object(tateros_app, "_save_last_llm_stats"),
            mock.patch.object(
                tateros_app,
                "get_llm_client_from_env",
                return_value=_AsyncContext(fake_client),
            ),
            mock.patch.object(
                tateros_app,
                "run_hydra_turn",
                new_callable=mock.AsyncMock,
                return_value={"content": "done"},
            ) as run_hydra,
            mock.patch.object(
                tateros_app.verba_registry_module,
                "ensure_verbas_loaded",
            ),
            mock.patch.object(
                tateros_app.verba_registry_module,
                "get_verba_registry",
                return_value={},
            ),
        ):
            result = await tateros_app._run_spud_link_native_hydra_completion(
                payload,
                [{"role": "user", "content": "hello"}],
                tools_enabled=False,
                request=request,
            )

        self.assertEqual(result["content"], "done")
        self.assertFalse(run_hydra.await_args.kwargs["origin"]["kernel_tools_enabled"])


if __name__ == "__main__":
    unittest.main()
