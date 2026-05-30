import asyncio
import json
import time
from typing import Any, Dict, List

from verba_result import action_failure, action_success
from web_research import research_web

from .loop_feedback import command_failure_context, execution_settings_summary, repeated_missing_executable
from .policy import display_agent_path, normalize_argv, resolve_spudex_cwd
from .runner import (
    append_session_log,
    finish_spudex_plan,
    read_spudex_logs,
    run_argv_in_session,
    set_spudex_memory_summary,
    set_spudex_verification,
    update_spudex_plan,
    update_spudex_session,
    write_spudex_file_in_session,
)
from .settings import get_spudex_settings
from .system_info import spudex_system_info


_ACTIONABLE_CONTINUATION_ACTIONS = {"command", "write_file", "search", "verify"}


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


def _recent_session_context(session_id: str) -> List[Dict[str, Any]]:
    rows = read_spudex_logs(session_id, after_seq=0, limit=350).get("entries") or []
    context: List[Dict[str, Any]] = []
    total_chars = 0
    for entry in rows[-160:]:
        if not isinstance(entry, dict):
            continue
        stream = str(entry.get("stream") or "log").strip().lower()
        text = str(entry.get("text") or "").strip()
        if not text:
            continue
        if stream not in {"user", "assistant", "command", "stdout", "stderr", "system", "policy", "search", "research", "file"}:
            continue
        clipped = text[:1200]
        total_chars += len(clipped)
        context.append(
            {
                "stream": stream,
                "text": clipped,
                "level": str(entry.get("level") or ""),
            }
        )
        if total_chars >= 16000:
            return context[-80:]
    return context[-80:]


def _decision_type(decision: Dict[str, Any]) -> str:
    token = str(decision.get("type") or decision.get("action") or "").strip().lower()
    if token in {"reply", "answer", "message", "done", "final"}:
        return "reply"
    if token in {"command", "run", "execute"}:
        return "command"
    if token in {"search", "research", "websearch", "web_search", "search_web", "research_web"}:
        return "search"
    if token in {"write_file", "file", "write", "create_file"}:
        return "write_file"
    if token in {"verify", "verification", "check"}:
        return "verify"
    if bool(decision.get("done")):
        return "reply"
    if decision.get("argv") is not None:
        return "command"
    if decision.get("path") is not None and decision.get("content") is not None:
        return "write_file"
    if decision.get("query") is not None:
        return "search"
    if decision.get("verify_argv") is not None:
        return "verify"
    return ""


async def _ai_should_continue_reply(
    *,
    llm_client: Any,
    settings: Dict[str, Any],
    user_message: str,
    assistant_reply: str,
    step: int,
    max_steps: int,
    ran_commands: List[Dict[str, Any]],
    ran_searches: List[Dict[str, Any]],
    wrote_files: List[Dict[str, Any]],
) -> Dict[str, Any]:
    has_observations = bool(ran_commands or ran_searches or wrote_files)
    judge_prompt = (
        "You are a strict continuation judge for Tater's Spudex Chat loop.\n"
        "Continuation means Spudex should take another action immediately in this same user turn. "
        "It does not mean staying alive, waiting for the user, or asking the user for a future task.\n"
        "Return exactly one JSON object with this shape: "
        "{\"continue\":true|false,\"reason\":\"...\",\"next_action\":\"none|command|write_file|search|verify\",\"instruction\":\"...\"}.\n"
        "Say continue=true when the assistant reply is only a promise, plan, or next-step narration and the user's task still needs action.\n"
        "Say continue=true when the user asked Spudex to take an action, but no command/file/search/verify action has happened yet.\n"
        "Say continue=true when the assistant asks the user for missing input, but the user's task can still start from a reasonable generic action such as search, inspection, or a local command.\n"
        "Say continue=false for greetings, small talk, capability questions, or normal conversational replies, even when the assistant is waiting for the user's next message.\n"
        "Say continue=false when the reply directly answers a question, asks for a future user task, reports completed work based on observations, no command is needed, or a genuinely specific private link/file/path/secret/detail is required before any useful action can happen.\n"
        "When continue=true, reason through what useful action is available now, then set next_action to command, write_file, search, or verify and give an instruction for the next Spudex model call without hard-coded provider-specific assumptions.\n"
        "The reason should briefly explain the semantic decision; the instruction should be a direct next-step instruction, not a transcript of hidden reasoning.\n"
        "Be conservative about stopping: if a useful next action can be taken now, continue.\n"
    )
    judge_payload = {
        "user_message": user_message,
        "assistant_reply": assistant_reply,
        "step": step,
        "max_steps": max_steps,
        "has_observations_this_turn": has_observations,
        "commands_this_turn": ran_commands[-3:],
        "searches_this_turn": ran_searches[-3:],
        "files_this_turn": wrote_files[-3:],
    }
    try:
        resp = await asyncio.wait_for(
            llm_client.chat(
                messages=[
                    {"role": "system", "content": judge_prompt},
                    {"role": "user", "content": json.dumps(judge_payload, ensure_ascii=False, default=str)},
                ],
                temperature=0.0,
            ),
            timeout=min(10, max(5, int(settings.get("command_timeout_sec") or 45) // 3)),
        )
        decision = _strict_json(_messages_content(resp))
        raw_continue = decision.get("continue")
        if isinstance(raw_continue, bool):
            return {
                "continue": raw_continue,
                "reason": str(decision.get("reason") or "AI continuation judge decision.").strip(),
                "next_action": str(decision.get("next_action") or "").strip(),
                "instruction": str(decision.get("instruction") or "").strip(),
                "source": "ai",
            }
        if isinstance(raw_continue, str) and raw_continue.strip().lower() in {"true", "false"}:
            return {
                "continue": raw_continue.strip().lower() == "true",
                "reason": str(decision.get("reason") or "AI continuation judge decision.").strip(),
                "next_action": str(decision.get("next_action") or "").strip(),
                "instruction": str(decision.get("instruction") or "").strip(),
                "source": "ai",
            }
    except Exception as exc:
        return {
            "continue": False,
            "reason": f"Continuation judge failed; stopping instead of using pattern fallback: {exc}",
            "next_action": "",
            "instruction": "",
            "source": "fallback",
        }
    return {
        "continue": False,
        "reason": "Continuation judge returned malformed JSON; stopping instead of using pattern fallback.",
        "next_action": "",
        "instruction": "",
        "source": "fallback",
    }


async def _ai_review_attempt(
    *,
    llm_client: Any,
    settings: Dict[str, Any],
    user_message: str,
    attempt: Dict[str, Any],
    ran_commands: List[Dict[str, Any]],
    ran_searches: List[Dict[str, Any]],
    wrote_files: List[Dict[str, Any]],
    loop_notes: List[str],
) -> str:
    review_prompt = (
        "You are Tater's Spudex attempt reviewer.\n"
        "Review the latest failed or weak action and produce one short model-facing note for the next planning step.\n"
        "Return exactly one JSON object: {\"note\":\"...\"}.\n"
        "The note should explain what was tried, why it did not answer the user's request, and what kind of meaningfully different next move should be considered.\n"
        "Do not reveal hidden chain-of-thought. Do not list hard-coded command names. Prefer tool/action categories, observed facts, and constraints from the payload.\n"
        "If the task is now blocked, say what specific user input is missing. If it is not blocked, say what should change about the approach.\n"
    )
    review_payload = {
        "user_message": user_message,
        "latest_attempt": attempt,
        "commands_this_turn": ran_commands[-5:],
        "searches_this_turn": ran_searches[-5:],
        "files_this_turn": wrote_files[-5:],
        "recent_loop_notes": loop_notes[-5:],
    }
    try:
        resp = await asyncio.wait_for(
            llm_client.chat(
                messages=[
                    {"role": "system", "content": review_prompt},
                    {"role": "user", "content": json.dumps(review_payload, ensure_ascii=False, default=str)},
                ],
                temperature=0.0,
            ),
            timeout=min(10, max(5, int(settings.get("command_timeout_sec") or 45) // 3)),
        )
        decision = _strict_json(_messages_content(resp))
        note = str(decision.get("note") or "").strip()
        return note[:900]
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


async def run_spudex_chat_turn(
    *,
    session_id: str,
    message: str,
    platform: str,
    llm_client: Any,
    redis_client: Any,
) -> Dict[str, Any]:
    settings = get_spudex_settings(redis_client)
    user_message = str(message or "").strip()
    if not user_message:
        return action_failure(code="spudex_chat_empty", message="Spudex chat message is required.")
    if llm_client is None or not hasattr(llm_client, "chat"):
        return action_failure(
            code="spudex_chat_model_unavailable",
            message="Spudex chat needs an available language model client.",
        )

    cwd_path = resolve_spudex_cwd(settings.get("default_cwd"))
    update_spudex_session(session_id, status="running", cwd=str(cwd_path), cwd_display=display_agent_path(cwd_path))
    append_session_log(session_id, stream="user", text=user_message, level="info")

    max_steps = max(1, int(settings.get("max_task_steps") or 6))
    final_text = ""
    ran_commands: List[Dict[str, Any]] = []
    ran_searches: List[Dict[str, Any]] = []
    wrote_files: List[Dict[str, Any]] = []
    loop_notes: List[str] = []
    system_prompt = (
        "You are the model inside Tater's Spudex Chat.\n"
        "You can talk normally, run one policy-controlled terminal command at a time, or ask Tater's websearch helper for command guidance.\n"
        "Return exactly one strict JSON object.\n"
        "Allowed shapes:\n"
        "{\"type\":\"reply\",\"message\":\"...\"}\n"
        "{\"type\":\"write_file\",\"path\":\"<relative/path>\",\"content\":\"...\",\"reason\":\"...\"}\n"
        "{\"type\":\"command\",\"argv\":[\"command\",\"arg\"],\"reason\":\"...\"}\n"
        "{\"type\":\"verify\",\"argv\":[\"command\",\"arg\"],\"reason\":\"...\"}\n"
        "{\"type\":\"search\",\"query\":\"how to check free memory on macOS command line\",\"reason\":\"...\"}\n"
        "Rules:\n"
        "- Include a compact plan array when the task has multiple steps: [{\"step\":\"Inspect\",\"status\":\"in_progress\"}]. Keep it updated as you work.\n"
        "- This is a conversation. Use prior session_context, including user corrections like 'no, do it like this'.\n"
        "- Use system_info to choose commands that fit the host OS and path style.\n"
        "- system_info may already include memory_total, memory_available, process_count_visible, and process_sample; answer simple host/process/memory questions from that data instead of running a command when it is enough.\n"
        "- If the user is asking a question or clarifying direction, reply without running a command.\n"
        "- Run commands only when command execution is actually useful.\n"
        "- If the user asks you to create, edit, run, host, serve, inspect, or verify something, your first response must usually be write_file, command, search, or verify JSON, not a conversational promise.\n"
        "- If the user asks you to look up, research, inspect examples, find docs, or gather public information, start with search JSON when no exact local file or URL is required.\n"
        "- Ask for missing details only when the task genuinely cannot start without that specific user-provided input.\n"
        "- Do not reply with 'I will run/check/try...' as a final answer. If you need to run/check/try something, return command, write_file, search, or verify now.\n"
        "- Before every action after the first, read loop_notes, commands_this_turn, searches_this_turn, and files_this_turn. Your next action must account for what already happened.\n"
        "- If the last attempt failed, returned no useful result, or did not answer the user's correction, explain in the action reason how the new approach differs or why stopping/asking is now appropriate.\n"
        "- After a command fails, never stop at 'I can try another way'. Return the next action JSON immediately.\n"
        "- If command_feedback shows error_code executable_not_found, do not retry that executable until you have installed it or completed another successful recovery action.\n"
        "- If an executable is missing and the task truly needs it, install the package that provides it when execution_settings allow installs/package managers or policy_disabled is true; then retry after the install succeeds.\n"
        "- If installing is not allowed or not worth it, choose an installed tool or a Python/stdlib fallback immediately. On Linux/Unraid, write a small Python script that reads /proc when ps, free, top, or similar utilities are unavailable.\n"
        "- Prefer direct terminal commands for ordinary inspection, filesystem, git, package, process, service, network, and OS checks.\n"
        "- Use a small Python script only when it materially simplifies multi-step logic, structured data parsing, calculations, generated file content, or when installed shell tools are missing/unavailable.\n"
        "- Do not use inline interpreter eval such as python -c; write a script file first, then run it.\n"
        "- Use search only when you are genuinely unsure about the right command or need current external instructions.\n"
        "- Use argv arrays only. Do not use shells, pipes, redirects, command separators, or inline eval.\n"
        "- Commands run from the configured working folder. Do not include or change cwd.\n"
        "- Command results come back in commands_this_turn with raw stdout, stderr, returncode, and status.\n"
        "- File writes come back in files_this_turn with path_display and bytes.\n"
        "- Web research results come back in searches_this_turn with answer, sources, and enough.\n"
        "- Before claiming a change is done, run a verify action when a practical command can confirm it.\n"
        "- If you start a long-running web server, set \"background\": true on the command so it stays attached to the session.\n"
        "- When you finish, include memory_summary with what changed and what the user should remember next time.\n"
        "- After command output gives enough information, reply with the result instead of running another command.\n"
    )

    async def _run_search_query(query: str, *, reason: str = "", decision: Dict[str, Any] | None = None) -> Dict[str, Any]:
        search_decision = decision or {}
        if reason:
            append_session_log(session_id, stream="assistant", text=reason, level="info")
        append_session_log(session_id, stream="search", text=f"? {query}", level="info")
        search_result = await research_web(
            query=query,
            question=user_message,
            llm_client=llm_client,
            max_results=_int_default(search_decision.get("max_results"), 5),
            max_pages=_int_default(search_decision.get("max_pages"), 3),
            provider=search_decision.get("provider") or search_decision.get("search_provider"),
            platform=platform,
        )
        search_data = search_result.get("data") if isinstance(search_result.get("data"), dict) else {}
        search_summary = str(search_result.get("summary_for_user") or search_data.get("answer") or "")
        append_session_log(
            session_id,
            stream="research",
            text=search_summary or str((search_result.get("error") or {}).get("message") or "Web research returned no answer."),
            level="info" if bool(search_result.get("ok")) else "error",
        )
        return {
            "query": query,
            "ok": bool(search_result.get("ok")),
            "enough": bool(search_data.get("enough")),
            "answer": str(search_data.get("answer") or search_summary or ""),
            "reason": str(search_data.get("reason") or ""),
            "missing": str(search_data.get("missing") or ""),
            "sources": list(search_data.get("sources") or [])[:5],
        }

    for step in range(max_steps):
        payload = {
            "message": user_message,
            "step": step + 1,
            "max_steps": max_steps,
            "working_folder": str(cwd_path),
            "system_info": spudex_system_info(),
            "execution_settings": execution_settings_summary(settings),
            "command_feedback": command_failure_context(ran_commands),
            "session_context": _recent_session_context(session_id),
            "commands_this_turn": ran_commands,
            "searches_this_turn": ran_searches,
            "files_this_turn": wrote_files,
            "loop_notes": loop_notes[-5:],
        }
        try:
            resp = await asyncio.wait_for(
                llm_client.chat(
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
                    ],
                    temperature=0.0,
                ),
                timeout=max(15, int(settings.get("command_timeout_sec") or 45)),
            )
        except asyncio.TimeoutError:
            final_text = "Spudex chat planning timed out while waiting for the model. Try again or use a smaller/faster Spudex model."
            append_session_log(session_id, stream="system", text=final_text, level="error")
            finish_spudex_plan(session_id, success=False)
            update_spudex_session(session_id, status="failed", finished_ts=time.time())
            return action_failure(code="spudex_chat_llm_timeout", message=final_text)
        except Exception as exc:
            final_text = f"Spudex chat planning failed: {exc}"
            append_session_log(session_id, stream="system", text=final_text, level="error")
            finish_spudex_plan(session_id, success=False)
            update_spudex_session(session_id, status="failed", finished_ts=time.time())
            return action_failure(code="spudex_chat_llm_failed", message=final_text)

        decision = _strict_json(_messages_content(resp))
        if isinstance(decision.get("plan"), list):
            update_spudex_plan(session_id, decision.get("plan"))
        kind = _decision_type(decision)
        if kind == "reply":
            final_text = str(decision.get("message") or decision.get("final") or decision.get("answer") or "").strip()
            if not final_text:
                final_text = "I do not need to run a command for that."
            if step < max_steps - 1:
                continuation = await _ai_should_continue_reply(
                    llm_client=llm_client,
                    settings=settings,
                    user_message=user_message,
                    assistant_reply=final_text,
                    step=step + 1,
                    max_steps=max_steps,
                    ran_commands=ran_commands,
                    ran_searches=ran_searches,
                    wrote_files=wrote_files,
                )
                if bool(continuation.get("continue")):
                    next_action = str(continuation.get("next_action") or "").strip().lower()
                    if next_action in {"research", "websearch", "web_search", "search_web", "research_web"}:
                        next_action = "search"
                    instruction = str(continuation.get("instruction") or "").strip()
                    if next_action not in _ACTIONABLE_CONTINUATION_ACTIONS:
                        set_spudex_memory_summary(session_id, decision.get("memory_summary") or final_text)
                        append_session_log(session_id, stream="assistant", text=final_text, level="info")
                        finish_spudex_plan(session_id, success=True)
                        update_spudex_session(session_id, status="succeeded", finished_ts=time.time())
                        return action_success(
                            facts={"session_id": session_id, "commands": len(ran_commands), "searches": len(ran_searches), "files": len(wrote_files)},
                            data={"session_id": session_id, "commands": ran_commands, "searches": ran_searches, "files": wrote_files},
                            summary_for_user=final_text,
                        )
                    instruction_note = ""
                    if instruction:
                        instruction_note = f" Suggested next_action={next_action}: {instruction}"
                    note = (
                        f"Continuation judge ({continuation.get('source')}) said to continue: {continuation.get('reason')}. "
                        "Return the next command, write_file, search, or verify object now."
                        f" {instruction_note}".rstrip()
                    )
                    loop_notes.append(note)
                    append_session_log(session_id, stream="system", text=f"Continuing Spudex loop: {continuation.get('reason')}", level="warning")
                    continue
            set_spudex_memory_summary(session_id, decision.get("memory_summary") or final_text)
            append_session_log(session_id, stream="assistant", text=final_text, level="info")
            finish_spudex_plan(session_id, success=True)
            update_spudex_session(session_id, status="succeeded", finished_ts=time.time())
            return action_success(
                facts={"session_id": session_id, "commands": len(ran_commands), "searches": len(ran_searches), "files": len(wrote_files)},
                data={"session_id": session_id, "commands": ran_commands, "searches": ran_searches, "files": wrote_files},
                summary_for_user=final_text,
            )

        if kind == "search":
            query = str(decision.get("query") or decision.get("question") or "").strip()
            if not query:
                final_text = "I wanted to search, but no search query was provided. Please rephrase the spudex request."
                append_session_log(session_id, stream="assistant", text=final_text, level="warning")
                finish_spudex_plan(session_id, success=False)
                update_spudex_session(session_id, status="failed", finished_ts=time.time())
                return action_failure(code="spudex_chat_bad_search", message=final_text)
            reason = str(decision.get("reason") or "").strip()
            search_row = await _run_search_query(query, reason=reason, decision=decision)
            ran_searches.append(search_row)
            if _search_attempt_needs_review(search_row) and step < max_steps - 1:
                review_note = await _ai_review_attempt(
                    llm_client=llm_client,
                    settings=settings,
                    user_message=user_message,
                    attempt={"action": "search", **search_row},
                    ran_commands=ran_commands,
                    ran_searches=ran_searches,
                    wrote_files=wrote_files,
                    loop_notes=loop_notes,
                )
                if review_note:
                    note = f"Attempt review after weak search result: {review_note}"
                    loop_notes.append(note)
                    append_session_log(session_id, stream="system", text=note, level="warning")
            continue

        if kind == "write_file":
            path = str(decision.get("path") or decision.get("filename") or "").strip()
            content = decision.get("content")
            if not path:
                final_text = "I wanted to write a file, but no file path was provided. Please rephrase the spudex request."
                append_session_log(session_id, stream="assistant", text=final_text, level="warning")
                finish_spudex_plan(session_id, success=False)
                update_spudex_session(session_id, status="failed", finished_ts=time.time())
                return action_failure(code="spudex_chat_bad_write_file", message=final_text)
            reason = str(decision.get("reason") or "").strip()
            if reason:
                append_session_log(session_id, stream="assistant", text=reason, level="info")
            next_cwd = resolve_spudex_cwd(settings.get("default_cwd"))
            write_result = write_spudex_file_in_session(
                session_id,
                path=path,
                content=content,
                cwd=next_cwd,
                append=bool(decision.get("append")),
                require_approval=bool(settings.get("require_file_approval")),
            )
            wrote_files.append(
                {
                    "path": path,
                    "ok": bool(write_result.get("ok")),
                    "path_display": str(write_result.get("path_display") or ""),
                    "bytes": int(write_result.get("bytes") or 0),
                    "append": bool(write_result.get("append")),
                    "error": write_result.get("error") if isinstance(write_result.get("error"), dict) else {},
                }
            )
            if not bool(write_result.get("ok")) and step < max_steps - 1:
                review_note = await _ai_review_attempt(
                    llm_client=llm_client,
                    settings=settings,
                    user_message=user_message,
                    attempt={"action": "write_file", **wrote_files[-1]},
                    ran_commands=ran_commands,
                    ran_searches=ran_searches,
                    wrote_files=wrote_files,
                    loop_notes=loop_notes,
                )
                if review_note:
                    note = f"Attempt review after failed file write: {review_note}"
                    loop_notes.append(note)
                    append_session_log(session_id, stream="system", text=note, level="warning")
            continue

        if kind == "verify":
            argv = normalize_argv(argv=decision.get("argv") or decision.get("verify_argv"))
            repeat_error = repeated_missing_executable(argv, ran_commands)
            if repeat_error:
                append_session_log(session_id, stream="system", text=str(repeat_error.get("message") or "Repeated missing executable skipped."), level="warning")
                ran_commands.append(
                    {
                        "argv": argv,
                        "cwd": str(resolve_spudex_cwd(settings.get("default_cwd"))),
                        "ok": False,
                        "status": "skipped",
                        "returncode": None,
                        "stdout": "",
                        "stderr": str(repeat_error.get("message") or ""),
                        "verification": True,
                        "error": repeat_error,
                    }
                )
                continue
            reason = str(decision.get("reason") or "").strip()
            if reason:
                append_session_log(session_id, stream="assistant", text=reason, level="info")
            next_cwd = resolve_spudex_cwd(settings.get("default_cwd"))
            result = await run_argv_in_session(
                session_id,
                argv=argv,
                cwd=next_cwd,
                settings=settings,
                capture_output=True,
                background=False,
            )
            summary = str(result.get("stdout") or result.get("stderr") or result.get("status") or "").strip()
            set_spudex_verification(
                session_id,
                command=argv,
                status="passed" if bool(result.get("ok")) else "failed",
                summary=summary,
                returncode=result.get("returncode"),
            )
            ran_commands.append(
                {
                    "argv": argv,
                    "cwd": str(next_cwd),
                    "ok": bool(result.get("ok")),
                    "status": result.get("status"),
                    "returncode": result.get("returncode"),
                    "stdout": str(result.get("stdout") or ""),
                    "stderr": str(result.get("stderr") or ""),
                    "verification": True,
                    "error": result.get("error") if isinstance(result.get("error"), dict) else {},
                }
            )
            if not bool(result.get("ok")) and step < max_steps - 1:
                review_note = await _ai_review_attempt(
                    llm_client=llm_client,
                    settings=settings,
                    user_message=user_message,
                    attempt={"action": "verify", **ran_commands[-1]},
                    ran_commands=ran_commands,
                    ran_searches=ran_searches,
                    wrote_files=wrote_files,
                    loop_notes=loop_notes,
                )
                if review_note:
                    note = f"Attempt review after failed verification: {review_note}"
                    loop_notes.append(note)
                    append_session_log(session_id, stream="system", text=note, level="warning")
            continue

        if kind != "command":
            final_text = "I could not decide whether to answer or run a command. Please rephrase the spudex request."
            append_session_log(session_id, stream="assistant", text=final_text, level="warning")
            finish_spudex_plan(session_id, success=False)
            update_spudex_session(session_id, status="failed", finished_ts=time.time())
            return action_failure(code="spudex_chat_bad_decision", message=final_text)

        argv = normalize_argv(argv=decision.get("argv"))
        repeat_error = repeated_missing_executable(argv, ran_commands)
        if repeat_error:
            append_session_log(session_id, stream="system", text=str(repeat_error.get("message") or "Repeated missing executable skipped."), level="warning")
            ran_commands.append(
                {
                    "argv": argv,
                    "cwd": str(resolve_spudex_cwd(settings.get("default_cwd"))),
                    "ok": False,
                    "status": "skipped",
                    "returncode": None,
                    "stdout": "",
                    "stderr": str(repeat_error.get("message") or ""),
                    "output_truncated": False,
                    "background": False,
                    "error": repeat_error,
                }
            )
            continue
        reason = str(decision.get("reason") or "").strip()
        if reason:
            append_session_log(session_id, stream="assistant", text=reason, level="info")
        next_cwd = resolve_spudex_cwd(settings.get("default_cwd"))
        result = await run_argv_in_session(
            session_id,
            argv=argv,
            cwd=next_cwd,
            settings=settings,
            capture_output=True,
            background=bool(decision.get("background")),
        )
        error = result.get("error") if isinstance(result.get("error"), dict) else {}
        ran_commands.append(
            {
                "argv": argv,
                "cwd": str(next_cwd),
                "ok": bool(result.get("ok")),
                "status": result.get("status"),
                "returncode": result.get("returncode"),
                "stdout": str(result.get("stdout") or ""),
                "stderr": str(result.get("stderr") or ""),
                "output_truncated": bool(result.get("output_truncated")),
                "background": bool(result.get("background")),
                "error": error,
            }
        )
        if not bool(result.get("ok")) and step < max_steps - 1:
            review_note = await _ai_review_attempt(
                llm_client=llm_client,
                settings=settings,
                user_message=user_message,
                attempt={"action": "command", **ran_commands[-1]},
                ran_commands=ran_commands,
                ran_searches=ran_searches,
                wrote_files=wrote_files,
                loop_notes=loop_notes,
            )
            if review_note:
                note = f"Attempt review after failed command: {review_note}"
                loop_notes.append(note)
                append_session_log(session_id, stream="system", text=note, level="warning")
        if not bool(result.get("ok")) and result.get("error"):
            continue

    final_text = "I paused at the configured Spudex Chat step limit. Tell me what to adjust and I can continue in this same session."
    append_session_log(session_id, stream="assistant", text=final_text, level="warning")
    finish_spudex_plan(session_id, success=False)
    update_spudex_session(session_id, status="succeeded", finished_ts=time.time())
    return action_success(
        facts={"session_id": session_id, "commands": len(ran_commands), "searches": len(ran_searches), "files": len(wrote_files), "step_limit": max_steps},
        data={"session_id": session_id, "commands": ran_commands, "searches": ran_searches, "files": wrote_files},
        summary_for_user=final_text,
    )
