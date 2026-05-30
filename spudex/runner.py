import asyncio
import difflib
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

from kernel_tools import AGENT_LAB_DIR

from .policy import display_agent_path, explain_policy_block, normalize_argv, resolve_spudex_cwd, resolve_spudex_file_path, validate_spudex_command
from .settings import get_spudex_settings


SPUDEX_DIR = AGENT_LAB_DIR / "spudex"
SESSIONS_DIR = SPUDEX_DIR / "sessions"

_ACTIVE_PROCESSES: dict[str, asyncio.subprocess.Process] = {}
_ACTIVE_TASKS: dict[str, asyncio.Task[Any]] = {}
_SESSION_LOCK = asyncio.Lock()
_DEFAULT_SUBPROCESS_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


def _now() -> float:
    return time.time()


def _ensure_dirs() -> None:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


def _subprocess_env() -> dict[str, str]:
    env = dict(os.environ)
    current_path = str(env.get("PATH") or "").strip()
    if current_path:
        parts = [part for part in current_path.split(os.pathsep) if part]
        for part in _DEFAULT_SUBPROCESS_PATH.split(":"):
            if part and part not in parts:
                parts.append(part)
        env["PATH"] = os.pathsep.join(parts)
    else:
        env["PATH"] = _DEFAULT_SUBPROCESS_PATH
    return env


def _command_start_error(exc: Exception, argv: List[str]) -> Dict[str, Any]:
    executable = Path(str(argv[0] or "")).name if argv else ""
    if isinstance(exc, FileNotFoundError):
        return {
            "code": "executable_not_found",
            "message": f"Executable not found: {executable or 'unknown'}",
            "executable": executable,
            "argv": list(argv),
            "retriable_without_change": False,
            "recovery": "Install the missing executable if installs are allowed and the task needs it, otherwise choose an installed tool or Python/stdlib fallback.",
        }
    return {
        "code": "start_failed",
        "message": str(exc) or "Failed to start command.",
        "exception_type": type(exc).__name__,
        "argv": list(argv),
    }


def _paths(session_id: str) -> tuple[Path, Path]:
    safe_id = "".join(ch for ch in str(session_id or "") if ch.isalnum() or ch in {"_", "-"})
    if not safe_id:
        safe_id = uuid.uuid4().hex
    return SESSIONS_DIR / f"{safe_id}.json", SESSIONS_DIR / f"{safe_id}.jsonl"


def _load_meta(session_id: str) -> Dict[str, Any]:
    meta_path, _ = _paths(session_id)
    if not meta_path.exists():
        return {}
    try:
        parsed = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        parsed = {}
    return parsed if isinstance(parsed, dict) else {}


def get_spudex_session(session_id: str) -> Dict[str, Any]:
    return _load_meta(session_id)


def register_spudex_task(session_id: str, task: asyncio.Task[Any]) -> None:
    clean_id = str(session_id or "").strip()
    if not clean_id:
        return
    _ACTIVE_TASKS[clean_id] = task
    task.add_done_callback(lambda _task: _ACTIVE_TASKS.pop(clean_id, None))


def _save_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_dirs()
    session_id = str(meta.get("id") or uuid.uuid4().hex)
    meta["id"] = session_id
    meta_path, _ = _paths(session_id)
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    return meta


def _clip_text(value: Any, limit: int = 24000) -> str:
    text = str(value if value is not None else "")
    if len(text) <= limit:
        return text
    return text[:limit] + "\n... [clipped]"


def _normalize_plan(plan: Any) -> list[Dict[str, Any]]:
    if not isinstance(plan, list):
        return []
    out: list[Dict[str, Any]] = []
    for index, item in enumerate(plan[:12], start=1):
        if isinstance(item, dict):
            step = str(item.get("step") or item.get("title") or item.get("task") or "").strip()
            status = str(item.get("status") or "pending").strip().lower()
            detail = str(item.get("detail") or item.get("note") or "").strip()
        else:
            step = str(item or "").strip()
            status = "pending"
            detail = ""
        if not step:
            continue
        if status not in {"pending", "in_progress", "completed", "blocked", "failed"}:
            status = "pending"
        out.append({"index": index, "step": step, "status": status, "detail": detail})
    return out


def update_spudex_plan(session_id: str, plan: Any = None, *, current_step: Any = None) -> list[Dict[str, Any]]:
    meta = _load_meta(session_id)
    current_plan = meta.get("plan") if isinstance(meta.get("plan"), list) else []
    next_plan = _normalize_plan(plan) if plan is not None else _normalize_plan(current_plan)
    current_text = str(current_step or "").strip()
    if current_text and not next_plan:
        next_plan = [{"index": 1, "step": current_text, "status": "in_progress", "detail": ""}]
    elif current_text:
        matched = False
        for item in next_plan:
            if str(item.get("step") or "").strip().lower() == current_text.lower():
                item["status"] = "in_progress"
                matched = True
                break
        if not matched:
            next_plan.append({"index": len(next_plan) + 1, "step": current_text, "status": "in_progress", "detail": ""})
    if next_plan:
        for index, item in enumerate(next_plan, start=1):
            item["index"] = index
        meta["plan"] = next_plan
        meta["updated_ts"] = _now()
        _save_meta(meta)
    return next_plan


def finish_spudex_plan(session_id: str, *, success: bool) -> list[Dict[str, Any]]:
    meta = _load_meta(session_id)
    next_plan = _normalize_plan(meta.get("plan") if isinstance(meta.get("plan"), list) else [])
    if not next_plan:
        return []
    final_status = "completed" if success else "failed"
    for item in next_plan:
        status = str(item.get("status") or "").strip().lower()
        if status in {"pending", "in_progress"}:
            item["status"] = final_status
    meta["plan"] = next_plan
    meta["updated_ts"] = _now()
    _save_meta(meta)
    return next_plan


def set_spudex_memory_summary(session_id: str, summary: Any) -> str:
    text = _clip_text(str(summary or "").strip(), 2000)
    if not text:
        return ""
    meta = _load_meta(session_id)
    meta["memory_summary"] = text
    meta["updated_ts"] = _now()
    _save_meta(meta)
    return text


def set_spudex_verification(session_id: str, *, command: Any = None, status: str = "", summary: Any = "", returncode: Any = None) -> Dict[str, Any]:
    meta = _load_meta(session_id)
    verification = {
        "command": " ".join(str(item) for item in command) if isinstance(command, list) else str(command or ""),
        "status": str(status or "").strip().lower() or "unknown",
        "summary": _clip_text(summary, 4000),
        "returncode": returncode,
        "ts": _now(),
    }
    meta["verification"] = verification
    meta["updated_ts"] = _now()
    _save_meta(meta)
    return verification


def _append_meta_list(session_id: str, key: str, item: Dict[str, Any], *, limit: int = 50) -> list[Dict[str, Any]]:
    meta = _load_meta(session_id)
    rows = meta.get(key) if isinstance(meta.get(key), list) else []
    rows = [row for row in rows if isinstance(row, dict)]
    rows.append(item)
    meta[key] = rows[-limit:]
    meta["updated_ts"] = _now()
    _save_meta(meta)
    return meta[key]


def _read_text_if_exists(path: Path) -> tuple[bool, str]:
    if not path.exists() or path.is_dir():
        return False, ""
    try:
        return True, path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return True, "[binary or unreadable file]"


def _file_diff(before: str, after: str, before_label: str, after_label: str) -> str:
    diff = difflib.unified_diff(
        before.splitlines(),
        after.splitlines(),
        fromfile=before_label,
        tofile=after_label,
        lineterm="",
    )
    return _clip_text("\n".join(diff), 40000)


def _record_file_change(
    session_id: str,
    *,
    path: Path,
    before_exists: bool,
    before: str,
    after: str,
    applied: bool,
    pending_id: str = "",
    append: bool = False,
) -> Dict[str, Any]:
    display_path = display_agent_path(path)
    change = {
        "id": pending_id or uuid.uuid4().hex,
        "path": str(path),
        "path_display": display_path,
        "before_exists": bool(before_exists),
        "applied": bool(applied),
        "pending": not bool(applied),
        "append": bool(append),
        "bytes": len(after.encode("utf-8")),
        "diff": _file_diff(before, after, f"{display_path} (before)", f"{display_path} (after)"),
        "content": after if not applied else "",
        "ts": _now(),
    }
    _append_meta_list(session_id, "file_changes", change, limit=80)
    return change


def _upsert_preview(session_id: str, url: str, *, source: str = "") -> None:
    clean_url = str(url or "").strip().rstrip(".,)")
    if not clean_url:
        return
    meta = _load_meta(session_id)
    rows = meta.get("previews") if isinstance(meta.get("previews"), list) else []
    previews = [row for row in rows if isinstance(row, dict)]
    if any(str(row.get("url") or "") == clean_url for row in previews):
        return
    previews.append({"url": clean_url, "source": str(source or ""), "ts": _now()})
    meta["previews"] = previews[-20:]
    meta["updated_ts"] = _now()
    _save_meta(meta)


def _detect_previews(session_id: str, text: str) -> None:
    raw = str(text or "")
    for match in re.findall(r"https?://[^\s'\"<>]+", raw):
        url = match.replace("0.0.0.0", "localhost")
        _upsert_preview(session_id, url, source="output")
    for match in re.findall(r"(?:localhost|127\.0\.0\.1|0\.0\.0\.0):(\d{2,5})", raw, flags=re.IGNORECASE):
        _upsert_preview(session_id, f"http://localhost:{match}", source="output")
    for match in re.findall(r"\b(?:port|listening on|running on)\s*:?\s*(\d{2,5})\b", raw, flags=re.IGNORECASE):
        _upsert_preview(session_id, f"http://localhost:{match}", source="output")


def _git_status_for(path: Path) -> Dict[str, Any]:
    try:
        root = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return {"ok": False}
    if root.returncode != 0:
        return {"ok": False}
    repo = root.stdout.strip()
    try:
        branch = subprocess.run(["git", "-C", repo, "branch", "--show-current"], capture_output=True, text=True, timeout=2)
        status = subprocess.run(["git", "-C", repo, "status", "--short"], capture_output=True, text=True, timeout=3)
    except Exception:
        return {"ok": False, "repo": repo}
    rows = [line for line in status.stdout.splitlines() if line.strip()]
    return {
        "ok": True,
        "repo": repo,
        "branch": branch.stdout.strip() or "detached",
        "dirty": bool(rows),
        "changed_count": len(rows),
        "changed_files": rows[:80],
    }


def append_session_log(session_id: str, *, stream: str, text: str, level: str = "info") -> Dict[str, Any]:
    _ensure_dirs()
    meta = _load_meta(session_id)
    seq = int(meta.get("log_seq") or 0) + 1
    entry = {
        "seq": seq,
        "ts": _now(),
        "stream": str(stream or "log"),
        "level": str(level or "info"),
        "text": str(text or ""),
    }
    _, log_path = _paths(session_id)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    meta["log_seq"] = seq
    meta["updated_ts"] = entry["ts"]
    _save_meta(meta)
    return entry


def read_spudex_logs(session_id: str, *, after_seq: int = 0, limit: int = 200) -> Dict[str, Any]:
    _, log_path = _paths(session_id)
    entries: list[Dict[str, Any]] = []
    max_limit = max(1, min(1000, int(limit or 200)))
    if log_path.exists():
        with log_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if not isinstance(entry, dict):
                    continue
                seq = int(entry.get("seq") or 0)
                if seq <= int(after_seq or 0):
                    continue
                entries.append(entry)
                if len(entries) >= max_limit:
                    break
    last_seq = int(entries[-1].get("seq") or after_seq or 0) if entries else int(after_seq or 0)
    return {"ok": True, "session_id": session_id, "entries": entries, "last_seq": last_seq}


def create_spudex_session(
    *,
    label: str,
    argv: List[str] | None = None,
    cwd: str = "",
    goal: str = "",
    source: str = "ui",
    platform: str = "webui",
) -> Dict[str, Any]:
    _ensure_dirs()
    session_id = uuid.uuid4().hex
    now = _now()
    meta = {
        "id": session_id,
        "label": str(label or "Spudex"),
        "argv": list(argv or []),
        "command": " ".join(str(item) for item in (argv or [])),
        "cwd": str(cwd or ""),
        "cwd_display": display_agent_path(cwd),
        "goal": str(goal or ""),
        "source": str(source or "ui"),
        "platform": str(platform or "webui"),
        "status": "queued",
        "returncode": None,
        "created_ts": now,
        "updated_ts": now,
        "started_ts": None,
        "finished_ts": None,
        "log_seq": 0,
    }
    _save_meta(meta)
    return meta


def update_spudex_session(session_id: str, **updates: Any) -> Dict[str, Any]:
    meta = _load_meta(session_id)
    if not meta:
        meta = {"id": str(session_id or uuid.uuid4().hex), "created_ts": _now()}
    for key, value in updates.items():
        meta[str(key)] = value
    meta["updated_ts"] = _now()
    return _save_meta(meta)


def list_spudex_sessions(*, limit: int = 80) -> list[Dict[str, Any]]:
    _ensure_dirs()
    rows: list[Dict[str, Any]] = []
    for meta_path in SESSIONS_DIR.glob("*.json"):
        try:
            parsed = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(parsed, dict):
            session_id = str(parsed.get("id") or "")
            parsed["active"] = session_id in _ACTIVE_PROCESSES or session_id in _ACTIVE_TASKS
            rows.append(parsed)
    rows.sort(key=lambda item: float(item.get("updated_ts") or item.get("created_ts") or 0), reverse=True)
    return rows[: max(1, int(limit or 80))]


def list_spudex_processes(*, model_only: bool = False) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    model_sources = {"spudex_chat", "hydra"}
    for session_id, process in list(_ACTIVE_PROCESSES.items()):
        meta = _load_meta(session_id)
        source = str(meta.get("source") or "").strip().lower()
        if model_only and source not in model_sources:
            continue
        rows.append(
            {
                "session_id": session_id,
                "pid": getattr(process, "pid", None),
                "label": str(meta.get("label") or meta.get("command") or "Spudex process"),
                "command": str(meta.get("command") or ""),
                "cwd": str(meta.get("cwd_display") or meta.get("cwd") or ""),
                "source": source or "unknown",
                "started_ts": meta.get("started_ts"),
                "updated_ts": meta.get("updated_ts"),
                "status": str(meta.get("status") or "running"),
            }
        )
    for session_id, task in list(_ACTIVE_TASKS.items()):
        if session_id in _ACTIVE_PROCESSES or task.done():
            continue
        meta = _load_meta(session_id)
        source = str(meta.get("source") or "").strip().lower()
        if model_only and source not in model_sources:
            continue
        rows.append(
            {
                "session_id": session_id,
                "pid": None,
                "label": str(meta.get("label") or meta.get("command") or "Spudex task"),
                "command": str(meta.get("command") or meta.get("goal") or ""),
                "cwd": str(meta.get("cwd_display") or meta.get("cwd") or ""),
                "source": source or "unknown",
                "started_ts": meta.get("started_ts") or meta.get("created_ts"),
                "updated_ts": meta.get("updated_ts"),
                "status": str(meta.get("status") or "running"),
            }
        )
    rows.sort(key=lambda item: float(item.get("started_ts") or item.get("updated_ts") or 0), reverse=True)
    return rows


def write_spudex_file_in_session(
    session_id: str,
    *,
    path: Any,
    content: Any,
    cwd: Path,
    append: bool = False,
    require_approval: bool = False,
) -> Dict[str, Any]:
    try:
        resolved = resolve_spudex_file_path(path, cwd=cwd)
    except Exception as exc:
        message = str(exc) or "Spudex file path was rejected."
        append_session_log(session_id, stream="policy", text=message, level="error")
        return {"ok": False, "session_id": session_id, "error": {"code": "path_rejected", "message": message}}

    try:
        data = str(content if content is not None else "")
        before_exists, before = _read_text_if_exists(resolved)
        after = before + data if append else data
        if require_approval:
            pending_id = uuid.uuid4().hex
            change = _record_file_change(
                session_id,
                path=resolved,
                before_exists=before_exists,
                before=before,
                after=after,
                applied=False,
                pending_id=pending_id,
                append=append,
            )
            append_session_log(session_id, stream="file", text=f"pending approval {display_agent_path(resolved)}", level="warning")
            return {
                "ok": False,
                "session_id": session_id,
                "pending": True,
                "change": change,
                "error": {"code": "file_approval_required", "message": "File write is waiting for approval in the Spudex UI."},
            }
        resolved.parent.mkdir(parents=True, exist_ok=True)
        if append:
            with resolved.open("a", encoding="utf-8") as handle:
                handle.write(data)
        else:
            resolved.write_text(data, encoding="utf-8")
        _record_file_change(
            session_id,
            path=resolved,
            before_exists=before_exists,
            before=before,
            after=after,
            applied=True,
            append=append,
        )
        display_path = display_agent_path(resolved)
        verb = "appended" if append else "wrote"
        append_session_log(session_id, stream="file", text=f"{verb} {display_path} ({len(data.encode('utf-8'))} bytes)", level="info")
        meta = _load_meta(session_id)
        meta.update({"status": "running", "updated_ts": _now()})
        _save_meta(meta)
        return {
            "ok": True,
            "session_id": session_id,
            "path": str(resolved),
            "path_display": display_path,
            "bytes": len(data.encode("utf-8")),
            "append": bool(append),
        }
    except Exception as exc:
        message = str(exc) or "Failed to write spudex file."
        append_session_log(session_id, stream="system", text=message, level="error")
        return {"ok": False, "session_id": session_id, "error": {"code": "write_failed", "message": message}}


def approve_spudex_file_change(session_id: str, change_id: str) -> Dict[str, Any]:
    meta = _load_meta(session_id)
    rows = meta.get("file_changes") if isinstance(meta.get("file_changes"), list) else []
    target: Dict[str, Any] | None = None
    for row in rows:
        if isinstance(row, dict) and str(row.get("id") or "") == str(change_id or ""):
            target = row
            break
    if not target:
        return {"ok": False, "error": {"code": "change_not_found", "message": "File change was not found."}}
    if not bool(target.get("pending")):
        return {"ok": True, "change": target}
    try:
        path = resolve_spudex_file_path(target.get("path") or target.get("path_display"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(target.get("content") or ""), encoding="utf-8")
    except Exception as exc:
        message = str(exc) or "Failed to approve file change."
        append_session_log(session_id, stream="file", text=message, level="error")
        return {"ok": False, "error": {"code": "approve_failed", "message": message}}
    target["pending"] = False
    target["applied"] = True
    target["approved_ts"] = _now()
    target["content"] = ""
    meta["file_changes"] = rows
    meta["updated_ts"] = _now()
    _save_meta(meta)
    append_session_log(session_id, stream="file", text=f"approved {target.get('path_display') or display_agent_path(path)}", level="info")
    return {"ok": True, "change": target}


def reject_spudex_file_change(session_id: str, change_id: str) -> Dict[str, Any]:
    meta = _load_meta(session_id)
    rows = meta.get("file_changes") if isinstance(meta.get("file_changes"), list) else []
    target: Dict[str, Any] | None = None
    for row in rows:
        if isinstance(row, dict) and str(row.get("id") or "") == str(change_id or ""):
            target = row
            break
    if not target:
        return {"ok": False, "error": {"code": "change_not_found", "message": "File change was not found."}}
    target["pending"] = False
    target["applied"] = False
    target["rejected_ts"] = _now()
    target["content"] = ""
    meta["file_changes"] = rows
    meta["updated_ts"] = _now()
    _save_meta(meta)
    append_session_log(session_id, stream="file", text=f"rejected {target.get('path_display') or target.get('path') or 'file change'}", level="warning")
    return {"ok": True, "change": target}


async def run_argv_in_session(
    session_id: str,
    *,
    argv: List[str],
    cwd: Path,
    settings: Dict[str, Any],
    capture_output: bool = False,
    background: bool = False,
) -> Dict[str, Any]:
    if bool(settings.get("policy_enabled", True)):
        validation = validate_spudex_command(argv, cwd, settings)
        if not bool(validation.get("ok")):
            append_session_log(session_id, stream="policy", text=str(validation.get("message") or "Command blocked."), level="error")
            explanation = explain_policy_block(validation)
            meta = _load_meta(session_id)
            meta.update(
                {
                    "status": "blocked",
                    "returncode": None,
                    "finished_ts": _now(),
                    "updated_ts": _now(),
                    "last_policy_block": explanation,
                }
            )
            _save_meta(meta)
            return {"ok": False, "session_id": session_id, "error": validation, "policy_explanation": explanation}
    elif not argv:
        append_session_log(session_id, stream="policy", text="No command was provided.", level="error")
        meta = _load_meta(session_id)
        meta.update({"status": "blocked", "returncode": None, "finished_ts": _now(), "updated_ts": _now()})
        _save_meta(meta)
        return {"ok": False, "session_id": session_id, "error": {"ok": False, "code": "empty_command", "message": "No command was provided."}}
    else:
        append_session_log(
            session_id,
            stream="policy",
            text="Spudex command safety policy is disabled for this run.",
            level="warning",
        )

    timeout_sec = max(5, int(settings.get("command_timeout_sec") or 45))
    meta = _load_meta(session_id)
    meta.update(
        {
            "status": "running",
            "argv": list(argv),
            "command": " ".join(str(item) for item in argv),
            "cwd": str(cwd),
            "cwd_display": display_agent_path(cwd),
            "started_ts": meta.get("started_ts") or _now(),
            "updated_ts": _now(),
        }
    )
    _save_meta(meta)
    append_session_log(session_id, stream="command", text=f"$ {' '.join(argv)}", level="info")
    _detect_previews(session_id, " ".join(str(item) for item in argv))

    captured_output: dict[str, list[str]] = {"stdout": [], "stderr": []}
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(cwd),
            env=_subprocess_env(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as exc:
        error = _command_start_error(exc, argv)
        message = str(error.get("message") or str(exc) or "Failed to start command.")
        append_session_log(session_id, stream="system", text=message, level="error")
        meta = _load_meta(session_id)
        meta.update({"status": "failed", "returncode": None, "finished_ts": _now(), "updated_ts": _now()})
        _save_meta(meta)
        result = {"ok": False, "session_id": session_id, "status": "failed", "returncode": None, "error": error}
        if capture_output:
            result.update({"stdout": "", "stderr": message, "output_truncated": False})
        return result
    _ACTIVE_PROCESSES[session_id] = process
    meta = _load_meta(session_id)
    meta.update({"pid": getattr(process, "pid", None), "updated_ts": _now()})
    _save_meta(meta)

    async def _pump(stream: Any, name: str) -> None:
        while True:
            chunk = await stream.readline()
            if not chunk:
                break
            text = chunk.decode("utf-8", errors="replace").rstrip("\n")
            if not text:
                continue
            if capture_output:
                captured_output[name].append(text)
            append_session_log(session_id, stream=name, text=text, level="info" if name == "stdout" else "error")
            _detect_previews(session_id, text)

    if background:
        async def _watch_background() -> None:
            returncode = 0
            status = "succeeded"
            try:
                await asyncio.gather(
                    _pump(process.stdout, "stdout"),
                    _pump(process.stderr, "stderr"),
                    process.wait(),
                )
                returncode = int(process.returncode or 0)
                status = "succeeded" if returncode == 0 else "failed"
            except Exception as exc:
                returncode = int(getattr(process, "returncode", None) or -1)
                status = "failed"
                append_session_log(session_id, stream="system", text=f"Background process failed: {exc}", level="error")
            finally:
                _ACTIVE_PROCESSES.pop(session_id, None)
                meta = _load_meta(session_id)
                meta.update({"status": status, "returncode": returncode, "finished_ts": _now(), "updated_ts": _now()})
                _save_meta(meta)
                append_session_log(session_id, stream="system", text=f"Background process finished with status {status} ({returncode}).", level="info")

        task = asyncio.create_task(_watch_background())
        _ACTIVE_TASKS[session_id] = task
        task.add_done_callback(lambda _task: _ACTIVE_TASKS.pop(session_id, None))
        append_session_log(session_id, stream="system", text="Background process started and will stay attached to this session.", level="info")
        return {"ok": True, "session_id": session_id, "status": "running", "returncode": None, "background": True}

    try:
        await asyncio.wait_for(
            asyncio.gather(
                _pump(process.stdout, "stdout"),
                _pump(process.stderr, "stderr"),
                process.wait(),
            ),
            timeout=timeout_sec,
        )
        returncode = int(process.returncode or 0)
        status = "succeeded" if returncode == 0 else "failed"
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        returncode = -9
        status = "timeout"
        append_session_log(session_id, stream="system", text=f"Command timed out after {timeout_sec}s.", level="error")
    finally:
        _ACTIVE_PROCESSES.pop(session_id, None)

    meta = _load_meta(session_id)
    meta.update(
        {
            "status": status,
            "returncode": returncode,
            "finished_ts": _now(),
            "updated_ts": _now(),
        }
    )
    _save_meta(meta)
    append_session_log(session_id, stream="system", text=f"Command finished with status {status} ({returncode}).", level="info")
    result = {"ok": returncode == 0, "session_id": session_id, "status": status, "returncode": returncode}
    if capture_output:
        result.update(
            {
                "stdout": "\n".join(captured_output["stdout"]),
                "stderr": "\n".join(captured_output["stderr"]),
                "output_truncated": False,
            }
        )
    return result


async def run_spudex_command_once(
    *,
    command: Any = None,
    argv: Any = None,
    cwd: Any = "",
    source: str = "hydra",
    platform: str = "webui",
    redis_client: Any = None,
    label: str = "Spudex command",
    background: bool = False,
) -> Dict[str, Any]:
    settings = get_spudex_settings(redis_client)
    parsed_argv = normalize_argv(command=command, argv=argv)
    resolved_cwd = resolve_spudex_cwd(cwd or settings.get("default_cwd"))
    session = create_spudex_session(
        label=label,
        argv=parsed_argv,
        cwd=str(resolved_cwd),
        source=source,
        platform=platform,
    )
    result = await run_argv_in_session(session["id"], argv=parsed_argv, cwd=resolved_cwd, settings=settings, background=background)
    logs = read_spudex_logs(session["id"], after_seq=0, limit=200)
    return {**result, "session": _load_meta(session["id"]), "logs": logs.get("entries") or []}


async def start_spudex_command(
    *,
    command: Any = None,
    argv: Any = None,
    cwd: Any = "",
    source: str = "ui",
    platform: str = "webui",
    redis_client: Any = None,
    label: str = "Spudex command",
    background: bool = False,
) -> Dict[str, Any]:
    settings = get_spudex_settings(redis_client)
    parsed_argv = normalize_argv(command=command, argv=argv)
    resolved_cwd = resolve_spudex_cwd(cwd or settings.get("default_cwd"))
    session = create_spudex_session(
        label=label,
        argv=parsed_argv,
        cwd=str(resolved_cwd),
        source=source,
        platform=platform,
    )
    task = asyncio.create_task(run_argv_in_session(session["id"], argv=parsed_argv, cwd=resolved_cwd, settings=settings, background=background))
    register_spudex_task(session["id"], task)
    return {"ok": True, "session": _load_meta(session["id"])}


async def stop_spudex_session(session_id: str) -> Dict[str, Any]:
    clean_id = str(session_id or "")
    process = _ACTIVE_PROCESSES.get(clean_id)
    task = _ACTIVE_TASKS.get(clean_id)
    if process is None:
        meta = _load_meta(session_id)
        if not meta:
            return {"ok": False, "error": {"code": "session_not_found", "message": "Spudex session was not found."}}
        if task is not None and not task.done():
            task.cancel()
            append_session_log(session_id, stream="system", text="Stop requested from UI.", level="warning")
            finish_spudex_plan(session_id, success=False)
            meta = _load_meta(session_id)
            meta.update({"status": "stopped", "finished_ts": _now(), "updated_ts": _now(), "returncode": None})
            _save_meta(meta)
            _ACTIVE_TASKS.pop(clean_id, None)
            return {"ok": True, "session": meta, "stopped": True}
        if str(meta.get("status") or "").strip().lower() in {"queued", "running"}:
            append_session_log(session_id, stream="system", text="No active Spudex process was found; marking stale session stopped.", level="warning")
            finish_spudex_plan(session_id, success=False)
            meta = _load_meta(session_id)
            meta.update({"status": "stopped", "finished_ts": _now(), "updated_ts": _now(), "returncode": None})
            _save_meta(meta)
            return {"ok": True, "session": meta, "stopped": True, "stale": True}
        return {"ok": True, "session": meta, "stopped": False}
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=2)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
    append_session_log(session_id, stream="system", text="Stop requested from UI.", level="warning")
    meta = _load_meta(session_id)
    finish_spudex_plan(session_id, success=False)
    meta = _load_meta(session_id)
    meta.update({"status": "stopped", "finished_ts": _now(), "updated_ts": _now(), "returncode": process.returncode})
    _save_meta(meta)
    _ACTIVE_PROCESSES.pop(session_id, None)
    return {"ok": True, "session": meta, "stopped": True}


async def close_spudex_session(session_id: str) -> Dict[str, Any]:
    clean_id = "".join(ch for ch in str(session_id or "") if ch.isalnum() or ch in {"_", "-"})
    if not clean_id:
        return {"ok": False, "error": {"code": "session_not_found", "message": "Spudex session was not found."}}

    meta_path, log_path = _paths(clean_id)
    meta = _load_meta(clean_id)
    task = _ACTIVE_TASKS.get(clean_id)
    has_session_files = meta_path.exists() or log_path.exists()
    if not meta and not has_session_files and clean_id not in _ACTIVE_PROCESSES and task is None:
        return {"ok": False, "error": {"code": "session_not_found", "message": "Spudex session was not found."}}

    status = str(meta.get("status") or "").strip().lower()
    if clean_id in _ACTIVE_PROCESSES or (task is not None and not task.done()) or status in {"queued", "running"}:
        await stop_spudex_session(clean_id)
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(task, timeout=2)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass

    _ACTIVE_PROCESSES.pop(clean_id, None)
    _ACTIVE_TASKS.pop(clean_id, None)
    for path in (meta_path, log_path):
        try:
            path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass

    return {"ok": True, "session_id": clean_id, "closed": True}


def spudex_payload(redis_client: Any = None) -> Dict[str, Any]:
    settings = get_spudex_settings(redis_client)
    sessions = list_spudex_sessions(limit=int(settings.get("max_sessions") or 80))
    model_processes = list_spudex_processes(model_only=True)
    active_ids = set(_ACTIVE_PROCESSES)
    active_ids.update(session_id for session_id, task in _ACTIVE_TASKS.items() if not task.done())
    return {
        "ok": True,
        "settings": settings,
        "agent_lab": str(AGENT_LAB_DIR),
        "sessions": sessions,
        "active_count": len(active_ids),
        "model_processes": model_processes,
        "model_process_count": len(model_processes),
        "git": _git_status_for(Path.cwd()),
    }
