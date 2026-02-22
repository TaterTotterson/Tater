import json
from typing import Any, Callable, Dict, Optional


def normalize_checker_kind(label: str) -> str:
    import re

    norm = re.sub(r"[\s\-]+", "_", str(label or "").strip().upper())
    if norm == "FINAL_ANSWER":
        return "FINAL_ANSWER"
    if norm == "RETRY_TOOL":
        return "RETRY_TOOL"
    if norm == "NEED_USER_INFO":
        return "NEED_USER_INFO"
    return "FINAL_ANSWER"


def parse_checker_decision(
    text: str,
    *,
    checker_decision_prefix_re: Any,
    parse_function_json_fn: Callable[[str], Any],
    is_tool_candidate_fn: Callable[[str], bool],
    normalize_checker_kind_fn: Callable[[str], str] = normalize_checker_kind,
) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {"kind": "FINAL_ANSWER", "text": ""}

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

    match = checker_decision_prefix_re.match(raw)
    if not match:
        if raw.startswith("{") and raw.endswith("}"):
            try:
                obj = json.loads(raw)
            except Exception:
                obj = None
            if isinstance(obj, dict):
                label = (
                    obj.get("kind")
                    or obj.get("action")
                    or obj.get("decision")
                    or obj.get("checker_action")
                )
                if isinstance(label, str) and label.strip():
                    kind = normalize_checker_kind_fn(label)
                    if kind == "RETRY_TOOL":
                        tool_call = _dict_tool_call(obj.get("tool_call") or obj.get("retry_tool") or obj.get("tool"))
                        if tool_call is None:
                            text_candidate = obj.get("text") or obj.get("content")
                            if isinstance(text_candidate, str):
                                parsed_candidate = parse_function_json_fn(text_candidate)
                                if isinstance(parsed_candidate, dict):
                                    tool_call = parsed_candidate
                        return {
                            "kind": "RETRY_TOOL",
                            "tool_call": tool_call,
                            "text": str(obj.get("text") or "").strip(),
                        }
                    body = str(
                        obj.get("text")
                        or obj.get("message")
                        or obj.get("final_answer")
                        or obj.get("answer")
                        or ""
                    ).strip()
                    if kind == "NEED_USER_INFO":
                        return {"kind": "NEED_USER_INFO", "text": body}
                    return {"kind": "FINAL_ANSWER", "text": body}
                if _state_like_payload(obj):
                    return {"kind": "FINAL_ANSWER", "text": ""}
        if is_tool_candidate_fn(raw):
            parsed = parse_function_json_fn(raw)
            if isinstance(parsed, dict):
                return {"kind": "RETRY_TOOL", "tool_call": parsed, "text": raw}
        return {"kind": "FINAL_ANSWER", "text": raw}

    kind = normalize_checker_kind_fn(str(match.group(1) or ""))
    body = str(match.group(2) or "").strip()
    if kind == "RETRY_TOOL":
        parsed = parse_function_json_fn(body)
        return {"kind": "RETRY_TOOL", "tool_call": parsed, "text": body}
    if kind == "NEED_USER_INFO":
        return {"kind": "NEED_USER_INFO", "text": body}
    return {"kind": "FINAL_ANSWER", "text": body}


async def run_checker(
    *,
    llm_client: Any,
    platform: str,
    current_user_text: str,
    resolved_user_text: str,
    agent_state: Optional[Dict[str, Any]],
    memory_context: Optional[Dict[str, Any]],
    planned_tool: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    draft_response: str,
    retry_allowed: bool,
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    coerce_non_negative_int_fn: Callable[[Any, int], int],
    short_text_fn: Callable[..., str],
    memory_context_default_summary_max_chars: int,
    configured_checker_max_tokens_fn: Callable[[], int],
    checker_system_prompt_fn: Callable[[str, bool], str],
    with_platform_preamble_fn: Callable[[list, str], list],
    parse_checker_decision_fn: Callable[[str], Dict[str, Any]],
    coerce_text_fn: Callable[[Any], str],
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    payload = {
        "current_user_message": current_user_text,
        "resolved_request_for_this_turn": resolved_user_text,
        "original_user_request": resolved_user_text,
        "agent_state": normalize_agent_state_fn(agent_state, resolved_user_text),
        "planned_tool": planned_tool,
        "tool_result": tool_result,
        "draft_response": draft_response,
    }
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
        token_limit = int(max_tokens) if max_tokens is not None else configured_checker_max_tokens_fn()
        response = await llm_client.chat(
            messages=with_platform_preamble_fn(
                [
                    {"role": "system", "content": checker_system_prompt_fn(platform, retry_allowed)},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                platform_preamble,
            ),
            max_tokens=max(1, token_limit),
            temperature=0.1,
        )
        text = coerce_text_fn((response.get("message", {}) or {}).get("content", "")).strip()
        return parse_checker_decision_fn(text)
    except Exception:
        return {"kind": "FINAL_ANSWER", "text": str(draft_response or "").strip()}
