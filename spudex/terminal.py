"""Full-access POSIX terminals, sharing Terminal task lifecycle and log transport."""
from __future__ import annotations

import asyncio
import errno
import os
import shlex
import signal
import struct
import time
from typing import Any

from . import runner
from .settings import get_spudex_settings
from .workspace import get_project

_TERMINALS: dict[str, dict[str, Any]] = {}
_START_LOCK = runner._SESSION_LOCK


class _TerminalProcess(runner._PosixSpawnProcess):
    def __init__(self, pid: int, master: int):
        super().__init__(pid, stdout=asyncio.StreamReader(), stderr=asyncio.StreamReader(), transports=[])
        self.master = master

    def _signal(self, sig: int) -> None:
        try:
            foreground = os.tcgetpgrp(self.master)
            if foreground > 0 and foreground not in {self.pid, os.getpgrp()}:
                os.killpg(foreground, sig)
        except (OSError, ProcessLookupError):
            pass
        try:
            os.killpg(self.pid, sig)
        except ProcessLookupError:
            pass

    def terminate(self) -> None:
        self._signal(signal.SIGHUP)

    def kill(self) -> None:
        self._signal(signal.SIGKILL)


def resize_terminal(session_id: str, cols: int, rows: int) -> None:
    import fcntl
    import termios
    terminal = _TERMINALS.get(session_id)
    if not terminal:
        raise ValueError("The interactive terminal is closed.")
    cols, rows = max(20, min(500, cols)), max(5, min(200, rows))
    fcntl.ioctl(terminal["master"], termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


async def write_terminal(session_id: str, data: str, *, cols: int = 0, rows: int = 0, redis_client: Any = None) -> dict[str, Any]:
    if not get_spudex_settings(redis_client).get("full_access"):
        raise PermissionError("Interactive shells require Full host access. Restricted command execution remains available.")
    terminal = _TERMINALS.get(session_id)
    if not terminal or terminal["process"].returncode is not None:
        raise ValueError("The interactive terminal is closed. Start another terminal to continue.")
    if len(data.encode("utf-8")) > 16384:
        raise ValueError("Terminal input is limited to 16 KiB per request.")
    async with terminal["input_lock"]:
        if cols and rows:
            resize_terminal(session_id, cols, rows)
        pending = data.encode("utf-8")
        deadline = time.monotonic() + 5
        while pending:
            if terminal["process"].returncode is not None or time.monotonic() > deadline:
                raise ValueError("The terminal did not accept input. Check its process or start a new terminal.")
            try:
                count = os.write(terminal["master"], pending)
                pending = pending[count:]
            except OSError as exc:
                if exc.errno not in {errno.EAGAIN, errno.EWOULDBLOCK, errno.EIO}:
                    raise
                await asyncio.sleep(.01)
    return {"ok": True}


def _trim_scrollback(session_id: str, limit: int) -> None:
    from collections import deque
    _, path = runner._paths(session_id)
    if not path.exists() or path.stat().st_size < limit * 2:
        return
    with runner._META_LOCK:
        retained: deque[bytes] = deque()
        total = 0
        with path.open("rb") as handle:
            for line in handle:
                retained.append(line)
                total += len(line)
                while total > limit and len(retained) > 1:
                    total -= len(retained.popleft())
        temp = path.with_suffix(".trim")
        temp.write_bytes(b"".join(retained))
        temp.replace(path)


async def start_terminal(*, project_id: str = "", cols: int = 100, rows: int = 26, redis_client: Any = None) -> dict[str, Any]:
    if os.name != "posix" or not hasattr(os, "posix_spawn"):
        raise ValueError("Interactive shells are available on macOS and Linux. Use Commands on this host.")
    settings = get_spudex_settings(redis_client)
    if not settings.get("full_access"):
        raise PermissionError("Interactive shells require Full host access. Restricted command execution remains available.")
    project = get_project(project_id, settings.get("default_cwd") or "agent_lab")
    async with _START_LOCK:
        if len(runner._ACTIVE_PROCESSES) >= int(settings.get("max_concurrent_processes") or 4):
            raise ValueError("The Terminal process limit has been reached. Stop an existing process first.")
        import pty
        master, slave = pty.openpty()
        slave_name = os.ttyname(slave)
        shell = os.environ.get("SHELL") or "/bin/sh"
        if not os.path.isfile(shell) or not os.path.isabs(shell):
            shell = "/bin/sh"
        # Reopen the slave AFTER setsid so the shell owns its controlling terminal.
        # posix_spawn avoids native macOS at-fork handlers in the embedded app.
        launcher = f"exec < {shlex.quote(slave_name)} > {shlex.quote(slave_name)} 2>&1; cd -- {shlex.quote(project['path'])} && exec {shlex.quote(shell)} -i"
        try:
            env = {**runner._subprocess_env(full_access=True), "TERM": "xterm-256color", "COLORTERM": "truecolor"}
            pid = os.posix_spawn("/bin/sh", ["/bin/sh", "-c", launcher], env, setsid=True, file_actions=[(os.POSIX_SPAWN_CLOSE, master), (os.POSIX_SPAWN_CLOSE, slave)], setsigdef=(signal.SIGINT, signal.SIGQUIT, signal.SIGPIPE))
        except BaseException:
            os.close(master)
            raise
        finally:
            os.close(slave)
        process = _TerminalProcess(pid, master)
        session_id = ""
        try:
            os.set_blocking(master, False)
            session = runner.create_spudex_session(label=f"Shell · {project['name']}", cwd=project["path"], source="interactive", platform="webui")
            session_id = session["id"]
            _TERMINALS[session_id] = {"master": master, "process": process, "input_lock": asyncio.Lock()}
            runner._ACTIVE_PROCESSES[session_id] = process
            session = runner.update_spudex_session(session_id, project=project, status="running", pid=pid, started_ts=time.time(), interactive=True)
            resize_terminal(session_id, cols, rows)
            runner.register_spudex_task(session_id, asyncio.create_task(_read_terminal(session_id, process, master, redis_client)))
            return {"ok": True, "session": session}
        except BaseException:
            # A failed setup must not leave an untracked host shell behind.
            await runner._terminate_process(process)
            _TERMINALS.pop(session_id, None)
            runner._ACTIVE_PROCESSES.pop(session_id, None)
            os.close(master)
            if session_id:
                runner.update_spudex_session(session_id, status="failed", finished_ts=time.time())
            raise


async def _read_terminal(session_id: str, process: _TerminalProcess, master: int, redis_client: Any) -> None:
    import codecs
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    waiting = asyncio.create_task(process.wait())
    access_check = time.monotonic()
    chunks = 0
    try:
        while True:
            if time.monotonic() - access_check >= 2:
                access_check = time.monotonic()
                if not get_spudex_settings(redis_client).get("full_access"):
                    runner.append_session_log(session_id, stream="system", text="Interactive shell closed because Full host access was disabled.", level="warning")
                    await runner._terminate_process(process)
            try:
                data = os.read(master, 4096)
            except BlockingIOError:
                data = None
            except OSError as exc:
                if exc.errno not in {errno.EIO, errno.EBADF}:
                    raise
                data = None
            if data:
                text = decoder.decode(data)
                if text:
                    runner.append_session_log(session_id, stream="terminal", text=text)
                chunks += 1
                if chunks % 64 == 0:
                    _trim_scrollback(session_id, 1024 * 1024)
            elif waiting.done():
                break
            else:
                await asyncio.sleep(.025)
        remainder = decoder.decode(b"", final=True)
        if remainder:
            runner.append_session_log(session_id, stream="terminal", text=remainder)
        await waiting
    except asyncio.CancelledError:
        await runner._terminate_process(process)
        raise
    finally:
        await runner._terminate_process(process)
        if not waiting.done():
            await waiting
        _TERMINALS.pop(session_id, None)
        if runner._ACTIVE_PROCESSES.get(session_id) is process:
            runner._ACTIVE_PROCESSES.pop(session_id, None)
        os.close(master)
        latest = runner.get_spudex_session(session_id)
        runner.update_spudex_session(session_id, status="stopped" if latest.get("status") == "stopped" else "succeeded" if process.returncode == 0 else "stopped", returncode=process.returncode, finished_ts=time.time())
