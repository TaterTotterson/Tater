import json
import time
from typing import Any, Dict, List, Optional

from verba_result import action_failure, action_success
from web_research import research_web

from .loop_feedback import command_failure_context, execution_settings_summary, repeated_missing_executable
from .policy import normalize_argv, resolve_spudex_cwd
from .runner import (
    append_session_log,
    create_spudex_session,
    read_spudex_logs,
    run_argv_in_session,
    set_spudex_memory_summary,
    set_spudex_verification,
    update_spudex_plan,
    update_spudex_session,
    write_spudex_file_in_session,
)
from .settings import spudex_enabled_for_platform, spudex_llm_overrides, get_spudex_settings
from .system_info import spudex_system_info


SPUDEX_TOOL_ROWS = [
    {
        "id": "run_terminal_task",
        "description": (
            "send a natural-language request to run console/terminal commands on the current PC where the assistant is running; "
            "use for local CPU/GPU/RAM/disk stats, OS/process diagnostics, scripts, file work, local servers, or other command-line tasks when no Verba fits"
        ),
        "usage": '{"function":"run_terminal_task","arguments":{"request":"<natural-language terminal task to complete>"}}',
    },
]


def spudex_hydra_tool_rows(*, platform: str, redis_client: Any = None) -> list[Dict[str, str]]:
    if not spudex_enabled_for_platform(platform, redis_client):
        return []
    return [dict(row) for row in SPUDEX_TOOL_ROWS]


def spudex_has_hydra_tool(tool_id: str, *, platform: str = "", redis_client: Any = None) -> bool:
    token = str(tool_id or "").strip()
    if token not in {str(row["id"]) for row in SPUDEX_TOOL_ROWS}:
        return False
    if platform:
        return spudex_enabled_for_platform(platform, redis_client)
    return bool(get_spudex_settings(redis_client).get("enabled"))


def spudex_tool_purpose_hint(tool_id: str) -> str:
    token = str(tool_id or "").strip()
    for row in SPUDEX_TOOL_ROWS:
        if row["id"] == token:
            return str(row.get("description") or "")
    return ""


def spudex_tool_usage_hint(tool_id: str) -> str:
    token = str(tool_id or "").strip()
    for row in SPUDEX_TOOL_ROWS:
        if row["id"] == token:
            return str(row.get("usage") or "")
    return ""


def _approval_required(settings: Dict[str, Any], argv: List[str], *, actor: str = "Hydra") -> Optional[Dict[str, Any]]:
    if not bool(settings.get("require_approval")):
        return None
    label = str(actor or "Hydra").strip() or "Hydra"
    return action_failure(
        code="spudex_approval_required",
        message=f"Spudex approval is enabled, so {label} did not run: {' '.join(argv)}",
        diagnosis={"argv": json.dumps(argv)},
        needs=["Turn off Spudex approval in the Spudex tab, or run the command manually from the Spudex tab."],
        say_hint="Explain that the spudex command needs approval in the Tater UI.",
    )


def _messages_content(resp: Any) -> str:
    if not isinstance(resp, dict):
        return ""
    message = resp.get("message")
    if isinstance(message, dict):
        return str(message.get("content") or "").strip()
    return ""


def _strict_json(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`").strip()
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _int_default(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


async def _ai_review_terminal_attempt(
    *,
    llm_client: Any,
    settings: Dict[str, Any],
    goal: str,
    attempt: Dict[str, Any],
    history: List[Dict[str, Any]],
    attempt_reviews: List[str],
) -> str:
    review_prompt = (
        "You are Tater's Spudex terminal attempt reviewer.\n"
        "Review the latest failed or weak action and produce one short model-facing note for the next planning step.\n"
        "Return exactly one JSON object: {\"note\":\"...\"}.\n"
        "The note should explain what was tried, why it did not answer the user's request, and what kind of meaningfully different next move should be considered.\n"
        "Do not reveal hidden chain-of-thought. Do not list hard-coded command names. Prefer tool/action categories, observed facts, and constraints from the payload.\n"
        "If the task is now blocked, say what specific user input is missing. If it is not blocked, say what should change about the approach.\n"
    )
    payload = {
        "goal": goal,
        "latest_attempt": attempt,
        "history": history[-8:],
        "attempt_reviews": attempt_reviews[-5:],
    }
    try:
        resp = await llm_client.chat(
            messages=[
                {"role": "system", "content": review_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
            ],
            temperature=0.0,
        )
        decision = _strict_json(_messages_content(resp))
        return str(decision.get("note") or "").strip()[:900]
    except Exception as exc:
        return f"Attempt review unavailable: {exc}"


def _search_attempt_needs_review(search_row: Dict[str, Any]) -> bool:
    if not bool(search_row.get("ok")):
        return True
    if bool(search_row.get("enough")):
        return False
    answer = str(search_row.get("answer") or "").strip()
    sources = search_row.get("sources") if isinstance(search_row.get("sources"), list) else []
    missing = str(search_row.get("missing") or "").strip()
    return not answer or not sources or bool(missing)


async def _run_spudex_task(
    *,
    args: Dict[str, Any],
    platform: str,
    llm_client: Any,
    redis_client: Any,
    source: str = "hydra",
    approval_actor: str = "Hydra",
    session_id: str = "",
) -> Dict[str, Any]:
    settings = get_spudex_settings(redis_client)
    goal = str(args.get("request") or args.get("goal") or args.get("task") or args.get("nl") or args.get("prompt") or "").strip()
    if not goal:
        return action_failure(
            code="run_terminal_task_missing_request",
            message="run_terminal_task needs arguments.request with the natural-language terminal task.",
            needs=["Provide arguments.request as a natural-language terminal task."],
            say_hint="Ask what terminal task should be done.",
        )
    if llm_client is None or not hasattr(llm_client, "chat"):
        return action_failure(
            code="run_terminal_task_model_unavailable",
            message="run_terminal_task needs an available language model client.",
            say_hint="Explain that terminal task execution needs the assistant model to plan commands.",
        )

    steps = max(1, int(settings.get("max_task_steps") or 6))
    cwd = resolve_spudex_cwd(settings.get("default_cwd"))
    if session_id:
        session = {"id": str(session_id)}
        update_spudex_session(str(session_id), status="running")
        append_session_log(str(session_id), stream="user", text=goal, level="info")
    else:
        session = create_spudex_session(label=f"Terminal task: {goal[:80]}", cwd=str(cwd), goal=goal, source=source, platform=platform)
        update_spudex_session(session["id"], status="running")
    history: list[Dict[str, Any]] = []
    attempt_reviews: list[str] = []

    system_prompt = (
        "You operate Tater's local Spudex terminal.\n"
        "Return exactly one strict JSON object.\n"
        "Allowed shapes:\n"
        "{\"done\":true,\"final\":\"...\"}\n"
        "{\"done\":false,\"action\":\"write_file\",\"path\":\"<relative/path>\",\"content\":\"...\",\"reason\":\"...\"}\n"
        "{\"done\":false,\"argv\":[\"command\",\"arg\"],\"reason\":\"...\"}\n"
        "{\"done\":false,\"action\":\"verify\",\"argv\":[\"command\",\"arg\"],\"reason\":\"...\"}\n"
        "{\"done\":false,\"action\":\"search\",\"query\":\"how to check free memory on Linux command line\",\"reason\":\"...\"}\n"
        "Rules:\n"
        "- Include a compact plan array when the task has multiple steps: [{\"step\":\"Inspect\",\"status\":\"in_progress\"}]. Keep it updated as you work.\n"
        "- Use system_info to choose commands that fit the host OS and path style.\n"
        "- If command_feedback shows error_code executable_not_found, do not retry that executable until you have installed it or completed another successful recovery action.\n"
        "- If an executable is missing and the task truly needs it, install the package that provides it when execution_settings allow installs/package managers or policy_disabled is true; then retry after the install succeeds.\n"
        "- If installing is not allowed or not worth it, choose an installed tool or a Python/stdlib fallback immediately. On Linux/Unraid, write a small Python script that reads /proc when ps, free, top, or similar utilities are unavailable.\n"
        "- Use search only when you are genuinely unsure about the right command or need current external instructions.\n"
        "- Prefer direct terminal commands for ordinary inspection, filesystem, git, package, process, service, network, and OS checks.\n"
        "- Use a small Python script only when it materially simplifies multi-step logic, structured data parsing, calculations, generated file content, or when installed shell tools are missing/unavailable.\n"
        "- Do not use inline interpreter eval such as python -c; write a script file first, then run it.\n"
        "- Use argv arrays only. Do not use shells, pipes, redirects, or command separators.\n"
        "- Commands run from the configured working folder. Do not include or change cwd.\n"
        "- Use small inspection commands before changing anything.\n"
        "- Before every action after the first, read history and attempt_reviews. Your next action must account for what already happened.\n"
        "- If the last attempt failed, returned no useful result, or did not answer the user's request, explain in the action reason how the new approach differs or why stopping/asking is now appropriate.\n"
        "- Before claiming a change is done, run a verify action when a practical command can confirm it.\n"
        "- If you start a long-running web server, set \"background\": true on the command so it stays attached to the session.\n"
        "- When you finish, include memory_summary with what changed and what should be remembered next time.\n"
        "- Stop when you have enough information or the task is done.\n"
    )

    final_text = ""
    for step in range(steps):
        payload = {
            "goal": goal,
            "step": step + 1,
            "max_steps": steps,
            "working_folder": str(cwd),
            "system_info": spudex_system_info(),
            "execution_settings": execution_settings_summary(settings),
            "command_feedback": command_failure_context(history),
            "history": history[-8:],
            "attempt_reviews": attempt_reviews[-5:],
        }
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
                ],
                temperature=0.0,
            )
        except Exception as exc:
            update_spudex_session(session["id"], status="failed", finished_ts=time.time())
            append_session_log(session["id"], stream="system", text=f"Terminal task planning failed: {exc}", level="error")
            return action_failure(code="run_terminal_task_llm_failed", message=f"Terminal task planning failed: {exc}")
        decision = _strict_json(_messages_content(resp))
        if isinstance(decision.get("plan"), list):
            update_spudex_plan(session["id"], decision.get("plan"))
        if bool(decision.get("done")):
            final_text = str(decision.get("final") or "Spudex task complete.").strip()
            set_spudex_memory_summary(session["id"], decision.get("memory_summary") or final_text)
            break
        action = str(decision.get("action") or decision.get("type") or "").strip().lower()
        if action in {"search", "research", "websearch", "web_search", "search_web", "research_web"} or (
            decision.get("query") is not None and decision.get("argv") is None
        ):
            query = str(decision.get("query") or decision.get("question") or "").strip()
            if not query:
                append_session_log(session["id"], stream="system", text="Spudex task requested web research with no query.", level="error")
                history.append({"action": "search", "ok": False, "error": "missing query"})
                continue
            reason = str(decision.get("reason") or "").strip()
            if reason:
                append_session_log(session["id"], stream="assistant", text=reason, level="info")
            append_session_log(session["id"], stream="search", text=f"? {query}", level="info")
            search_result = await research_web(
                query=query,
                question=goal,
                llm_client=llm_client,
                max_results=_int_default(decision.get("max_results"), 5),
                max_pages=_int_default(decision.get("max_pages"), 3),
                provider=decision.get("provider") or decision.get("search_provider"),
                platform=platform,
            )
            search_data = search_result.get("data") if isinstance(search_result.get("data"), dict) else {}
            search_summary = str(search_result.get("summary_for_user") or search_data.get("answer") or "")
            append_session_log(
                session["id"],
                stream="research",
                text=search_summary or str((search_result.get("error") or {}).get("message") or "Web research returned no answer."),
                level="info" if bool(search_result.get("ok")) else "error",
            )
            search_row = {
                "action": "search",
                "query": query,
                "ok": bool(search_result.get("ok")),
                "enough": bool(search_data.get("enough")),
                "answer": str(search_data.get("answer") or search_summary or ""),
                "reason": str(search_data.get("reason") or ""),
                "missing": str(search_data.get("missing") or ""),
                "sources": list(search_data.get("sources") or [])[:5],
            }
            history.append(search_row)
            if _search_attempt_needs_review(search_row) and step < steps - 1:
                review_note = await _ai_review_terminal_attempt(
                    llm_client=llm_client,
                    settings=settings,
                    goal=goal,
                    attempt=search_row,
                    history=history,
                    attempt_reviews=attempt_reviews,
                )
                if review_note:
                    note = f"Attempt review after weak search result: {review_note}"
                    attempt_reviews.append(note)
                    append_session_log(session["id"], stream="system", text=note, level="warning")
            continue
        if action in {"write_file", "file", "write", "create_file"} or (
            decision.get("path") is not None and decision.get("content") is not None and decision.get("argv") is None
        ):
            path = str(decision.get("path") or decision.get("filename") or "").strip()
            if not path:
                append_session_log(session["id"], stream="system", text="Spudex task requested file write with no path.", level="error")
                history.append({"action": "write_file", "ok": False, "error": "missing path"})
                continue
            approval = _approval_required(settings, ["write_file", path], actor=approval_actor)
            if approval:
                append_session_log(session["id"], stream="system", text=str(approval.get("error", {}).get("message") or "Spudex approval required."), level="warning")
                update_spudex_session(session["id"], status="blocked", finished_ts=time.time())
                return approval
            reason = str(decision.get("reason") or "").strip()
            if reason:
                append_session_log(session["id"], stream="assistant", text=reason, level="info")
            next_cwd = resolve_spudex_cwd(settings.get("default_cwd"))
            write_result = write_spudex_file_in_session(
                session["id"],
                path=path,
                content=decision.get("content"),
                cwd=next_cwd,
                append=bool(decision.get("append")),
                require_approval=bool(settings.get("require_file_approval")),
            )
            history.append(
                {
                    "action": "write_file",
                    "path": path,
                    "ok": bool(write_result.get("ok")),
                    "path_display": str(write_result.get("path_display") or ""),
                    "bytes": int(write_result.get("bytes") or 0),
                    "append": bool(write_result.get("append")),
                    "error": write_result.get("error") if isinstance(write_result.get("error"), dict) else {},
                }
            )
            if not bool(write_result.get("ok")) and step < steps - 1:
                review_note = await _ai_review_terminal_attempt(
                    llm_client=llm_client,
                    settings=settings,
                    goal=goal,
                    attempt=history[-1],
                    history=history,
                    attempt_reviews=attempt_reviews,
                )
                if review_note:
                    note = f"Attempt review after failed file write: {review_note}"
                    attempt_reviews.append(note)
                    append_session_log(session["id"], stream="system", text=note, level="warning")
            continue
        if action in {"verify", "verification", "check"} or decision.get("verify_argv") is not None:
            argv = normalize_argv(argv=decision.get("argv") or decision.get("verify_argv"))
            repeat_error = repeated_missing_executable(argv, history)
            if repeat_error:
                append_session_log(session["id"], stream="system", text=str(repeat_error.get("message") or "Repeated missing executable skipped."), level="warning")
                history.append({"action": "verify", "argv": argv, "ok": False, "status": "skipped", "returncode": None, "error": repeat_error})
                continue
            approval = _approval_required(settings, argv, actor=approval_actor)
            if approval:
                append_session_log(session["id"], stream="system", text=str(approval.get("error", {}).get("message") or "Spudex approval required."), level="warning")
                update_spudex_session(session["id"], status="blocked", finished_ts=time.time())
                return approval
            reason = str(decision.get("reason") or "").strip()
            if reason:
                append_session_log(session["id"], stream="assistant", text=reason, level="info")
            next_cwd = resolve_spudex_cwd(settings.get("default_cwd"))
            result = await run_argv_in_session(session["id"], argv=argv, cwd=next_cwd, settings=settings, capture_output=True)
            summary = str(result.get("stdout") or result.get("stderr") or result.get("status") or "").strip()
            set_spudex_verification(
                session["id"],
                command=argv,
                status="passed" if bool(result.get("ok")) else "failed",
                summary=summary,
                returncode=result.get("returncode"),
            )
            history.append(
                {
                    "action": "verify",
                    "argv": argv,
                    "ok": bool(result.get("ok")),
                    "status": result.get("status"),
                    "returncode": result.get("returncode"),
                    "error": result.get("error") if isinstance(result.get("error"), dict) else {},
                    "recent_output": summary[-2000:],
                }
            )
            if not bool(result.get("ok")) and step < steps - 1:
                review_note = await _ai_review_terminal_attempt(
                    llm_client=llm_client,
                    settings=settings,
                    goal=goal,
                    attempt=history[-1],
                    history=history,
                    attempt_reviews=attempt_reviews,
                )
                if review_note:
                    note = f"Attempt review after failed verification: {review_note}"
                    attempt_reviews.append(note)
                    append_session_log(session["id"], stream="system", text=note, level="warning")
            continue
        argv = normalize_argv(argv=decision.get("argv"))
        repeat_error = repeated_missing_executable(argv, history)
        if repeat_error:
            append_session_log(session["id"], stream="system", text=str(repeat_error.get("message") or "Repeated missing executable skipped."), level="warning")
            history.append(
                {
                    "argv": argv,
                    "ok": False,
                    "status": "skipped",
                    "returncode": None,
                    "background": False,
                    "error": repeat_error,
                    "recent_output": str(repeat_error.get("message") or ""),
                }
            )
            continue
        approval = _approval_required(settings, argv, actor=approval_actor)
        if approval:
            append_session_log(session["id"], stream="system", text=str(approval.get("error", {}).get("message") or "Spudex approval required."), level="warning")
            update_spudex_session(session["id"], status="blocked", finished_ts=time.time())
            return approval
        next_cwd = resolve_spudex_cwd(settings.get("default_cwd"))
        result = await run_argv_in_session(session["id"], argv=argv, cwd=next_cwd, settings=settings, background=bool(decision.get("background")))
        logs = read_spudex_logs(session["id"], after_seq=0, limit=80).get("entries") or []
        history.append(
            {
                "argv": argv,
                "ok": bool(result.get("ok")),
                "status": result.get("status"),
                "returncode": result.get("returncode"),
                "background": bool(result.get("background")),
                "error": result.get("error") if isinstance(result.get("error"), dict) else {},
                "recent_output": "\n".join(str(entry.get("text") or "") for entry in logs[-12:]),
            }
        )
        if not bool(result.get("ok")) and step < steps - 1:
            review_note = await _ai_review_terminal_attempt(
                llm_client=llm_client,
                settings=settings,
                goal=goal,
                attempt=history[-1],
                history=history,
                attempt_reviews=attempt_reviews,
            )
            if review_note:
                note = f"Attempt review after failed command: {review_note}"
                attempt_reviews.append(note)
                append_session_log(session["id"], stream="system", text=note, level="warning")
    else:
        final_text = "Spudex task stopped at the configured step limit."

    if final_text:
        set_spudex_memory_summary(session["id"], final_text)
        append_session_log(session["id"], stream="assistant", text=final_text, level="info")
    update_spudex_session(session["id"], status="succeeded", finished_ts=time.time())
    logs = read_spudex_logs(session["id"], after_seq=0, limit=200).get("entries") or []
    return action_success(
        facts={"session_id": session["id"], "steps": len(history), "final": final_text},
        data={"session_id": session["id"], "history": history, "logs": logs},
        summary_for_user=final_text or "Spudex task complete.",
        say_hint="Summarize the spudex task result briefly and mention the session id if useful.",
    )


async def run_spudex_loop_task(
    *,
    args: Dict[str, Any],
    platform: str,
    llm_client: Any,
    redis_client: Any,
    source: str = "spudex_chat",
    approval_actor: str = "Spudex chat",
    session_id: str = "",
) -> Dict[str, Any]:
    return await _run_spudex_task(
        args=args,
        platform=platform,
        llm_client=llm_client,
        redis_client=redis_client,
        source=source,
        approval_actor=approval_actor,
        session_id=session_id,
    )


async def run_spudex_hydra_tool(
    *,
    tool_id: str,
    args: Dict[str, Any],
    platform: str,
    origin: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
    redis_client: Any = None,
) -> Optional[Dict[str, Any]]:
    del origin
    token = str(tool_id or "").strip()
    if token not in {str(row["id"]) for row in SPUDEX_TOOL_ROWS}:
        return None
    if not spudex_enabled_for_platform(platform, redis_client):
        return action_failure(
            code="spudex_disabled",
            message="Tater Spudex is disabled for this platform.",
            say_hint="Explain that the Spudex feature must be enabled in the Spudex tab first.",
        )

    payload = args if isinstance(args, dict) else {}
    if token == "run_terminal_task":
        overrides = spudex_llm_overrides(redis_client)
        if overrides.get("host") or overrides.get("model"):
            from helpers import get_llm_client_from_env

            llm_kwargs: Dict[str, Any] = {
                "host": overrides.get("host"),
                "model": overrides.get("model"),
                "redis_conn": redis_client,
            }
            if overrides.get("provider") and overrides.get("model"):
                llm_kwargs["provider"] = overrides.get("provider")
            async with get_llm_client_from_env(**llm_kwargs) as spudex_llm_client:
                return await _run_spudex_task(args=payload, platform=platform, llm_client=spudex_llm_client, redis_client=redis_client)
        return await _run_spudex_task(args=payload, platform=platform, llm_client=llm_client, redis_client=redis_client)
    return None
