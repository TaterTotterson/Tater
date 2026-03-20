import json
from typing import Any, Callable, Dict, Optional


def normalize_minos_decision(label: str) -> str:
    import re

    norm = re.sub(r"[\s\-]+", "_", str(label or "").strip().upper())
    if norm in {"CONTINUE", "NEXT", "PROCEED"}:
        return "CONTINUE"
    if norm in {"RETRY", "RETRY_TOOL", "TRY_AGAIN"}:
        return "RETRY"
    if norm in {"ASK_USER", "NEED_USER_INFO", "ASK", "QUESTION"}:
        return "ASK_USER"
    if norm in {"FAIL", "FAILED", "BLOCKED", "CANNOT_COMPLETE"}:
        return "FAIL"
    if norm in {"FINAL", "FINAL_ANSWER", "DONE", "COMPLETE"}:
        return "FINAL"
    return "FINAL"


def normalize_minos_kind(label: str) -> str:
    decision = normalize_minos_decision(label)
    if decision == "RETRY":
        return "RETRY_TOOL"
    if decision == "ASK_USER":
        return "NEED_USER_INFO"
    return "FINAL_ANSWER"


def parse_minos_decision(
    text: str,
    *,
    minos_decision_prefix_re: Any,
    parse_function_json_fn: Callable[[str], Any],
    is_tool_candidate_fn: Callable[[str], bool],
    normalize_minos_decision_fn: Callable[[str], str] = normalize_minos_decision,
) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {"decision": "FINAL", "kind": "FINAL_ANSWER", "reason": "", "text": ""}

    def _compat_kind(decision: str) -> str:
        normalized = str(decision or "").strip().upper()
        if normalized == "RETRY":
            return "RETRY_TOOL"
        if normalized == "ASK_USER":
            return "NEED_USER_INFO"
        return "FINAL_ANSWER"

    def _decision_payload(
        *,
        decision: str,
        reason: str = "",
        next_action: str = "",
        repair: str = "",
        question: str = "",
        text: str = "",
        tool_call: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "decision": str(decision or "FINAL").strip().upper() or "FINAL",
            "kind": _compat_kind(decision),
            "reason": str(reason or "").strip(),
            "next_action": str(next_action or "").strip(),
            "repair": str(repair or "").strip(),
            "question": str(question or "").strip(),
            "text": str(text or "").strip(),
        }
        if isinstance(tool_call, dict):
            payload["tool_call"] = tool_call
        return payload

    def _state_like_payload(obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        keys = {str(k or "").strip().lower() for k in obj.keys() if str(k or "").strip()}
        state_keys = {"goal", "plan", "facts", "open_questions", "next_step", "tool_history"}
        if len(keys & state_keys) >= 3:
            return True
        content_val = obj.get("content")
        if isinstance(content_val, str):
            content_text = content_val.strip()
            if content_text.startswith("{") and '"goal"' in content_text and '"plan"' in content_text and '"facts"' in content_text:
                return True
        return False

    def _dict_tool_call(candidate: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(candidate, dict):
            return None
        func = str(candidate.get("function") or candidate.get("tool") or "").strip()
        if not func:
            return None
        args = candidate.get("arguments")
        if not isinstance(args, dict):
            args = {}
        return {"function": func, "arguments": args}

    def _extract_dict_text(obj: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    match = minos_decision_prefix_re.match(raw)
    if match:
        decision = normalize_minos_decision_fn(str(match.group(1) or ""))
        body = str(match.group(2) or "").strip()
        if decision == "RETRY":
            parsed = parse_function_json_fn(body)
            if isinstance(parsed, dict):
                return _decision_payload(
                    decision="RETRY",
                    reason="retry_requested",
                    repair="Retry the current step.",
                    text=body,
                    tool_call=parsed,
                )
            return _decision_payload(
                decision="RETRY",
                reason="retry_requested",
                repair=body or "Retry the current step.",
                text=body,
            )
        if decision == "ASK_USER":
            return _decision_payload(
                decision="ASK_USER",
                reason="needs_user_input",
                question=body,
                text=body,
            )
        if decision == "FAIL":
            return _decision_payload(
                decision="FAIL",
                reason=body or "execution_failed",
                text=body,
            )
        if decision == "CONTINUE":
            return _decision_payload(
                decision="CONTINUE",
                reason=body or "continue",
                next_action=body or "Proceed to next step.",
                text=body,
            )
        return _decision_payload(
            decision="FINAL",
            reason=body or "complete",
            text=body,
        )

    if raw.startswith("{") and raw.endswith("}"):
        try:
            obj = json.loads(raw)
        except Exception:
            obj = None
        if isinstance(obj, dict):
            label = (
                obj.get("decision")
                or obj.get("kind")
                or obj.get("action")
                or obj.get("checker_action")
            )
            if isinstance(label, str) and label.strip():
                decision = normalize_minos_decision_fn(label)
                reason = _extract_dict_text(obj, "reason", "message", "text", "content")
                next_action = _extract_dict_text(obj, "next_action", "next", "action")
                repair = _extract_dict_text(obj, "repair", "fix", "hint")
                question = _extract_dict_text(obj, "question", "ask", "prompt")
                body_text = _extract_dict_text(obj, "text", "message", "final_answer", "answer", "content")
                tool_call = _dict_tool_call(obj.get("tool_call") or obj.get("retry_tool") or obj.get("tool"))
                if tool_call is None:
                    text_candidate = obj.get("text") or obj.get("content")
                    if isinstance(text_candidate, str):
                        parsed_candidate = parse_function_json_fn(text_candidate)
                        if isinstance(parsed_candidate, dict):
                            tool_call = parsed_candidate
                if decision == "RETRY" and not repair:
                    repair = body_text or "Retry the current step."
                if decision == "ASK_USER" and not question:
                    question = body_text
                if decision == "FINAL" and not reason:
                    reason = "complete"
                return _decision_payload(
                    decision=decision,
                    reason=reason,
                    next_action=next_action,
                    repair=repair,
                    question=question,
                    text=body_text,
                    tool_call=tool_call,
                )
            if _state_like_payload(obj):
                return _decision_payload(decision="FINAL", reason="complete", text="")

    if is_tool_candidate_fn(raw):
        parsed = parse_function_json_fn(raw)
        if isinstance(parsed, dict):
            return _decision_payload(
                decision="RETRY",
                reason="retry_requested",
                repair="Retry the current step.",
                text=raw,
                tool_call=parsed,
            )

    return _decision_payload(decision="FINAL", reason=raw or "complete", text=raw)


async def run_minos_validation(
    *,
    llm_client: Any,
    platform: str,
    current_user_text: str,
    resolved_user_text: str,
    agent_state: Optional[Dict[str, Any]],
    memory_context: Optional[Dict[str, Any]],
    available_artifacts: Optional[list[Dict[str, Any]]],
    current_step: Optional[Dict[str, Any]],
    goal: str,
    planned_tool: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    draft_response: str,
    retry_count: int,
    retry_allowed: bool,
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    coerce_non_negative_int_fn: Callable[[Any, int], int],
    short_text_fn: Callable[..., str],
    memory_context_default_summary_max_chars: int,
    configured_minos_max_tokens_fn: Callable[[], Optional[int]],
    minos_system_prompt_fn: Callable[[str, bool], str],
    with_platform_preamble_fn: Callable[[list, str], list],
    parse_minos_decision_fn: Callable[[str], Dict[str, Any]],
    coerce_text_fn: Callable[[Any], str],
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    normalized_goal = short_text_fn(goal or resolved_user_text, limit=240) or str(resolved_user_text or "").strip()
    payload = {
        "current_user_message": current_user_text,
        "resolved_request_for_this_turn": resolved_user_text,
        "original_user_request": resolved_user_text,
        "goal": normalized_goal,
        "agent_state": normalize_agent_state_fn(agent_state, resolved_user_text),
        "current_step": current_step if isinstance(current_step, dict) else None,
        "retry_count": int(max(0, retry_count or 0)),
        "retry_allowed": bool(retry_allowed),
        "planned_tool": planned_tool,
        "tool_result": tool_result,
        "draft_response": draft_response,
    }
    if isinstance(available_artifacts, list) and available_artifacts:
        payload["available_artifacts"] = [dict(item) for item in available_artifacts if isinstance(item, dict)]
    if isinstance(memory_context, dict) and memory_context:
        user_ctx = memory_context.get("user") if isinstance(memory_context.get("user"), dict) else {}
        room_ctx = memory_context.get("room") if isinstance(memory_context.get("room"), dict) else {}
        summary_limit = coerce_non_negative_int_fn(
            memory_context.get("_summary_char_limit"),
            memory_context_default_summary_max_chars,
        ) or memory_context_default_summary_max_chars
        summary_limit = max(128, min(12000, summary_limit))
        payload["memory_context"] = {
            "user_memory": short_text_fn(user_ctx.get("summary"), limit=summary_limit),
            "room_memory": short_text_fn(room_ctx.get("summary"), limit=summary_limit),
        }
    try:
        token_limit = int(max_tokens) if max_tokens is not None else configured_minos_max_tokens_fn()
        chat_kwargs: Dict[str, Any] = {
            "messages": with_platform_preamble_fn(
                [
                    {"role": "system", "content": minos_system_prompt_fn(platform, retry_allowed)},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                platform_preamble,
            ),
            "temperature": 0.1,
            "max_tokens": (max(1, int(token_limit)) if token_limit is not None else None),
        }
        response = await llm_client.chat(**chat_kwargs)
        text = coerce_text_fn((response.get("message", {}) or {}).get("content", "")).strip()
        return parse_minos_decision_fn(text)
    except Exception:
        return {
            "decision": "FINAL",
            "kind": "FINAL_ANSWER",
            "reason": "validator_error",
            "text": str(draft_response or "").strip(),
        }
