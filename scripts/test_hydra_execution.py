import asyncio
import importlib.util
from pathlib import Path
import unittest


_MODULE_PATH = Path(__file__).resolve().parents[1] / "hydra" / "hydra_execution.py"
_SPEC = importlib.util.spec_from_file_location("hydra_execution_under_test", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
hydra_execution = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hydra_execution)


class ToolProgressOverlapTests(unittest.IsolatedAsyncioTestCase):
    async def test_tool_result_returns_while_progress_is_still_running(self) -> None:
        progress_started = asyncio.Event()
        release_progress = asyncio.Event()

        async def progress() -> float:
            progress_started.set()
            await release_progress.wait()
            return 17.0

        async def tool():
            await progress_started.wait()
            return {"ok": True}

        result, progress_task = await hydra_execution.execute_while_progress_runs(
            progress_awaitable=progress(),
            tool_awaitable=tool(),
        )

        self.assertEqual(result, {"ok": True})
        self.assertFalse(progress_task.done())

        release_progress.set()
        self.assertEqual(await progress_task, 17.0)

    async def test_tool_failure_cancels_unfinished_progress(self) -> None:
        progress_cancelled = asyncio.Event()

        async def progress() -> float:
            try:
                await asyncio.Event().wait()
            finally:
                progress_cancelled.set()
            return 0.0

        async def tool():
            await asyncio.sleep(0)
            raise RuntimeError("tool failed")

        with self.assertRaisesRegex(RuntimeError, "tool failed"):
            await hydra_execution.execute_while_progress_runs(
                progress_awaitable=progress(),
                tool_awaitable=tool(),
            )

        self.assertTrue(progress_cancelled.is_set())


if __name__ == "__main__":
    unittest.main()
