"""Project context and bounded inspection for the Terminal workspace."""
from __future__ import annotations

import difflib
import hashlib
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from . import runner
from .policy import resolve_spudex_cwd

MODES = {"ask", "plan", "build"}
_REGISTRY_LOCK = threading.RLock()
_SKIP = {".git", "node_modules", ".venv", "venv", "__pycache__", "build", "dist"}


def normalize_mode(value: Any) -> str:
    mode = str(value or "build").strip().lower()
    if mode not in MODES:
        raise ValueError("Task mode must be Ask, Plan, or Build.")
    return mode


def project_row(path: Path, name: str = "") -> dict[str, Any]:
    path = path.expanduser().resolve()
    return {"id": hashlib.sha256(os.fsencode(str(path))).hexdigest()[:24], "name": name.strip()[:100] or path.name or str(path), "path": str(path)}


def _registry_path() -> Path:
    return runner.SPUDEX_DIR / "projects.json"


def list_projects(default_cwd: str = "agent_lab") -> list[dict[str, Any]]:
    default = project_row(resolve_spudex_cwd(default_cwd))
    default["name"] = "Agent Lab" if default["path"] == str(runner.AGENT_LAB_DIR.resolve()) else default["name"]
    default["default"] = True
    try:
        saved = json.loads(_registry_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        saved = []
    rows = {default["id"]: default}
    for row in saved if isinstance(saved, list) else []:
        if not isinstance(row, dict) or not row.get("path"):
            continue
        normalized = project_row(Path(row["path"]), str(row.get("name") or ""))
        rows.setdefault(normalized["id"], normalized)
    for session in runner.list_spudex_sessions(limit=80):
        stored = session.get("project")
        candidate = stored.get("path") if isinstance(stored, dict) else session.get("cwd") if session.get("source") == "spudex_chat" else ""
        if candidate and Path(candidate).is_dir():
            inferred = project_row(Path(candidate), str(stored.get("name") or "") if isinstance(stored, dict) else "")
            rows.setdefault(inferred["id"], inferred)
    return list(rows.values())


def add_project(path: str, name: str = "", default_cwd: str = "agent_lab") -> dict[str, Any]:
    if not path.strip():
        raise ValueError("Enter an existing project folder.")
    folder = resolve_spudex_cwd(path)
    if not folder.is_dir():
        raise ValueError("Choose an existing project folder.")
    row = project_row(folder, name)
    with _REGISTRY_LOCK:
        rows = {item["id"]: item for item in list_projects(default_cwd)}
        rows[row["id"]] = row
        dest = _registry_path()
        dest.parent.mkdir(parents=True, exist_ok=True)
        pending = dest.with_suffix(".tmp")
        pending.write_text(json.dumps(list(rows.values()), indent=2), encoding="utf-8")
        pending.replace(dest)
    return row


def get_project(project_id: str = "", default_cwd: str = "agent_lab") -> dict[str, Any]:
    rows = list_projects(default_cwd)
    row = next((item for item in rows if item["id"] == project_id), None) if project_id else rows[0]
    if row is None:
        raise ValueError("This project is no longer available. Open its folder again.")
    if not Path(row["path"]).is_dir():
        raise ValueError("The project folder is unavailable. Reconnect its volume or choose another folder.")
    return row


def session_project(session: dict[str, Any], default_cwd: str = "agent_lab") -> dict[str, Any]:
    stored = session.get("project")
    if isinstance(stored, dict) and stored.get("path"):
        path = Path(stored["path"]).expanduser().resolve()
        if not path.is_dir():
            raise ValueError("This task's project folder is unavailable.")
        return project_row(path, str(stored.get("name") or ""))
    return project_row(resolve_spudex_cwd(session.get("cwd") or default_cwd))


def _git(path: Path, *args: str, timeout: float = 3) -> subprocess.CompletedProcess[str]:
    # Disable executable diff drivers and pagers supplied by repository configuration.
    return subprocess.run(["git", "--no-pager", "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null", "-C", str(path), *args], capture_output=True, text=True, errors="replace", timeout=timeout, env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"})


def project_changes(project: dict[str, Any]) -> dict[str, Any]:
    path = Path(project["path"])
    try:
        root_result = _git(path, "rev-parse", "--show-toplevel")
        if root_result.returncode:
            return {"ok": False, "project": project, "files": [], "message": "This folder does not use Git. Task file edits still appear below."}
        repo = Path(root_result.stdout.strip()).resolve()
        branch = _git(repo, "branch", "--show-current").stdout.strip() or "detached"
        result = _git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        if result.returncode:
            raise ValueError(result.stderr.strip() or "Git status failed.")
        records = iter(result.stdout.split("\0"))
        files = []
        total = 0
        head = _git(repo, "rev-parse", "--verify", "HEAD")
        for record in records:
            if len(record) < 4:
                continue
            status, relative = record[:2], record[3:]
            if "R" in status or "C" in status:
                next(records, None)
            resolved = (repo / relative).resolve()
            if not resolved.is_relative_to(repo) or not resolved.is_relative_to(path):
                continue
            total += 1
            if len(files) >= 60:
                continue
            oversized = resolved.is_file() and resolved.stat().st_size > 524288
            if resolved.is_symlink() or (repo / relative).is_symlink():
                diff = "Symbolic link changed."
            elif oversized:
                diff = "This file is too large for inline review. Open the full file in your editor."
            elif status == "??":
                try:
                    with resolved.open("rb") as handle:
                        raw = handle.read(49152)
                    diff = "Binary file added." if b"\x00" in raw else "".join(difflib.unified_diff([], raw.decode("utf-8", "replace").splitlines(True), fromfile="/dev/null", tofile=relative))
                except OSError:
                    diff = "File is unavailable."
            else:
                # HEAD includes both staged and unstaged changes. Unborn repositories have no HEAD.
                args = ("diff", "--no-ext-diff", "--no-textconv", "HEAD", "--", relative) if not head.returncode else ("diff", "--no-ext-diff", "--no-textconv", "--cached", "--", relative)
                diff_result = _git(repo, *args)
                if diff_result.returncode:
                    raise ValueError(diff_result.stderr.strip() or "Git diff failed.")
                diff = diff_result.stdout
                if head.returncode:
                    diff += _git(repo, "diff", "--no-ext-diff", "--no-textconv", "--", relative).stdout
            clipped = oversized or len(diff) > 49152
            diff = diff[:49152]
            files.append({"id": relative, "path": str(resolved), "name": relative, "status": status.strip(), "diff": diff, "truncated": clipped, "added": sum(line.startswith("+") and not line.startswith("+++") for line in diff.splitlines()), "removed": sum(line.startswith("-") and not line.startswith("---") for line in diff.splitlines())})
        return {"ok": True, "project": project, "repo": str(repo), "branch": branch, "files": files, "dirty": bool(files), "total_count": total, "truncated": total > len(files), "scope": "project_working_tree", "checked_ts": time.time()}
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        return {"ok": False, "project": project, "files": [], "message": str(exc)}


def inspect_project(project: dict[str, Any], action: str, *, path: str = ".", query: str = "") -> dict[str, Any]:
    """Read-only actions: no shell, subprocesses, or project writes."""
    root = Path(project["path"]).resolve()
    target = (root / path).resolve()
    if not target.is_relative_to(root):
        return {"ok": False, "error": "Inspection paths must stay inside the selected project."}
    try:
        if action == "read_file":
            with target.open("rb") as handle:
                raw = handle.read(48001)
            if b"\x00" in raw:
                return {"ok": False, "error": "This is a binary file."}
            return {"ok": True, "path": str(target.relative_to(root)), "content": raw[:48000].decode("utf-8", "replace"), "truncated": len(raw) > 48000}
        if not target.is_dir():
            return {"ok": False, "error": "Choose an existing project directory."}
        rows: list[Any] = []
        scanned = 0
        for directory, dirs, names in os.walk(target, followlinks=False):
            dirs[:] = sorted(name for name in dirs if name not in _SKIP and not (Path(directory) / name).is_symlink())
            for name in sorted(names):
                item = Path(directory) / name
                if item.is_symlink() or not item.resolve().is_relative_to(root):
                    continue
                scanned += 1
                relative = str(item.relative_to(root))
                if action == "list_files":
                    rows.append(relative)
                elif action == "search_files" and query:
                    try:
                        with item.open("rb") as handle:
                            raw = handle.read(48000)
                        if b"\x00" not in raw:
                            for line_no, line in enumerate(raw.decode("utf-8", "replace").splitlines(), 1):
                                if query.casefold() in line.casefold():
                                    rows.append({"path": relative, "line": line_no, "text": line[:500]})
                                    if len(rows) >= 100:
                                        break
                    except OSError:
                        pass
                if len(rows) >= 100 or scanned >= 2000:
                    return {"ok": True, "results": rows, "truncated": True}
        return {"ok": True, "results": rows, "truncated": False}
    except OSError as exc:
        return {"ok": False, "error": str(exc)}


def project_context(project: dict[str, Any]) -> dict[str, Any]:
    context = {"project": project, "files": inspect_project(project, "list_files").get("results", [])}
    guidance = []
    for name in ("AGENTS.md", "README.md"):
        result = inspect_project(project, "read_file", path=name)
        if result.get("ok"):
            guidance.append({"path": name, "content": result["content"][:6000]})
    context["project_documents"] = guidance
    return context


def prepare_task(*, message: str, session_id: str = "", project_id: str = "", mode: str | None = None, resume: bool = False, default_cwd: str = "agent_lab") -> tuple[dict[str, Any], str]:
    session = runner.get_spudex_session(session_id) if session_id else {}
    if session_id and (not session or session.get("source") != "spudex_chat"):
        raise ValueError("This coding task was not found. Select a task or start a new one.")
    active = runner._ACTIVE_TASKS.get(session_id)
    if (active and not active.done()) or session_id in runner._ACTIVE_PROCESSES:
        raise RuntimeError("This task is already running. Stop it or wait for it to finish.")
    if resume:
        if not session:
            raise ValueError("Select an unfinished task to continue.")
        message = str(session.get("last_user_message") or session.get("goal") or "").strip()
        if not message:
            raise ValueError("This task has no saved request. Send a message to start it.")
    if not message.strip():
        raise ValueError("Describe the task or question first.")
    project = session_project(session, default_cwd) if session else get_project(project_id, default_cwd)
    if project_id and project_id != project["id"]:
        raise ValueError("A task stays attached to its project. Start a new task in the other folder.")
    selected_mode = normalize_mode(mode or session.get("mode"))
    if not session:
        session = runner.create_spudex_session(label=message[:80], cwd=project["path"], goal=message, source="spudex_chat", platform="webui")
    label = session.get("label") or message[:80]
    if session.get("status") == "draft" or label == "New Terminal chat":
        label = message[:80]
    session = runner.update_spudex_session(session["id"], label=label, goal=session.get("goal") or message, last_user_message=message, project=project, mode=selected_mode, cwd=project["path"], status="queued", finished_ts=None, verification=None, progress={"phase": "queued", "text": "Waiting for the Terminal model", "step": 0})
    return session, message
