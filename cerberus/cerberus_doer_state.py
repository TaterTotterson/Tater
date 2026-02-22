import json
import re
from typing import Any, Callable, Dict, List, Optional


def state_add_line(
    state_list: List[str],
    line: str,
    *,
    max_items: int,
    short_text_fn: Callable[..., str],
) -> List[str]:
    text = short_text_fn(" ".join(str(line or "").split()), limit=150)
    if not text:
        return state_list
    lowered = text.lower()
    for existing in state_list:
        if str(existing).strip().lower() == lowered:
            return state_list
    out = list(state_list)
    out.append(text)
    return out[-max_items:]


def tool_history_line(
    *,
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    short_text_fn: Callable[..., str],
) -> str:
    func = ""
    if isinstance(tool_call, dict):
        func = str(tool_call.get("function") or "").strip()
    func = func or "tool"
    ok = bool((tool_result or {}).get("ok"))
    summary = short_text_fn((tool_result or {}).get("summary_for_user"), limit=120)
    if not summary:
        errors = (tool_result or {}).get("errors")
        if isinstance(errors, list) and errors:
            summary = short_text_fn(errors[0], limit=120)
    if not summary:
        summary = "no summary"
    status = "ok" if ok else "failed"
    return f"{func}:{status}:{summary}"


def compact_tool_result_for_doer(
    tool_result: Optional[Dict[str, Any]],
    *,
    short_text_fn: Callable[..., str],
) -> Dict[str, Any]:
    source = tool_result if isinstance(tool_result, dict) else {}
    out: Dict[str, Any] = {
        "ok": bool(source.get("ok")),
        "summary_for_user": short_text_fn(source.get("summary_for_user"), limit=260),
    }
    errors: List[str] = []
    raw_errors = source.get("errors")
    if isinstance(raw_errors, list):
        for item in raw_errors:
            line = short_text_fn(item, limit=180)
            if line:
                errors.append(line)
            if len(errors) >= 3:
                break
    if errors:
        out["errors"] = errors
    data = source.get("data")
    if isinstance(data, dict) and data:
        preview: Dict[str, Any] = {}
        for key in list(data.keys())[:8]:
            value = data.get(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                preview[key] = value
                continue
            try:
                preview[key] = short_text_fn(json.dumps(value, ensure_ascii=False), limit=160)
            except Exception:
                preview[key] = short_text_fn(str(value), limit=160)
        out["data_preview"] = preview
    return out


async def run_doer_state_update(
    *,
    llm_client: Any,
    platform: str,
    user_request: str,
    prior_state: Optional[Dict[str, Any]],
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    max_tokens: Optional[int],
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    configured_doer_max_tokens_fn: Callable[[], int],
    coerce_text_fn: Callable[[Any], str],
    first_json_object_fn: Callable[[str], Optional[Dict[str, Any]]],
    state_add_line_fn: Callable[[List[str], str, int], List[str]],
    tool_history_line_fn: Callable[[Optional[Dict[str, Any]], Optional[Dict[str, Any]]], str],
    short_text_fn: Callable[..., str],
    is_low_information_text_fn: Callable[[Any], bool],
    state_list_fn: Callable[[Any, int, int], List[str]],
) -> Dict[str, Any]:
    previous = normalize_agent_state_fn(prior_state, fallback_goal=user_request)
    payload = {
        "platform": platform,
        "user_request": str(user_request or ""),
        "prior_state": previous,
        "tool_call": {
            "function": str((tool_call or {}).get("function") or "").strip(),
            "arguments": (tool_call or {}).get("arguments")
            if isinstance((tool_call or {}).get("arguments"), dict)
            else {},
        },
        "tool_result": compact_tool_result_for_doer(tool_result, short_text_fn=short_text_fn),
    }
    prompt = (
        "You are the Doer head in a Planner/Doer/Critic loop.\n"
        "Update only the agent state.\n"
        "Return exactly one compact JSON object with keys:\n"
        "goal, plan, facts, open_questions, next_step, tool_history\n"
        "Rules:\n"
        "- Use short plain text snippets.\n"
        "- Keep plan as the remaining checklist of explicit user-requested actions.\n"
        "- If multiple actions were requested, keep unfinished items in plan and set next_step to the next unfinished action.\n"
        "- Remove plan items that are already completed.\n"
        "- Keep facts stable and deterministic.\n"
        "- Record completion facts only when tool_result.ok is true; for failures, keep blocker details in open_questions.\n"
        "- Keep open_questions only for real blockers.\n"
        "- next_step is a short tool sketch or empty.\n"
        "- No markdown."
    )
    merged: Dict[str, Any] = dict(previous)
    try:
        token_limit = int(max_tokens) if max_tokens is not None else configured_doer_max_tokens_fn()
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=max(1, token_limit),
            temperature=0.1,
        )
        text = coerce_text_fn((response.get("message", {}) or {}).get("content", "")).strip()
        patch_state = first_json_object_fn(text)
        if isinstance(patch_state, dict):
            merged = normalize_agent_state_fn(
                patch_state,
                fallback_goal=previous.get("goal") or user_request,
            )
    except Exception:
        merged = dict(previous)

    merged["tool_history"] = state_add_line_fn(
        list(merged.get("tool_history") or []),
        tool_history_line_fn(tool_call, tool_result),
        8,
    )

    summary = short_text_fn((tool_result or {}).get("summary_for_user"), limit=150)
    if summary and not is_low_information_text_fn(summary):
        merged["facts"] = state_add_line_fn(list(merged.get("facts") or []), summary, 8)

    if not bool((tool_result or {}).get("ok")):
        errors = (tool_result or {}).get("errors")
        if isinstance(errors, list) and errors:
            merged["open_questions"] = state_add_line_fn(
                list(merged.get("open_questions") or []),
                short_text_fn(errors[0], limit=150),
                4,
            )
    elif merged.get("open_questions"):
        merged["open_questions"] = state_list_fn(merged.get("open_questions"), 4, 140)

    return normalize_agent_state_fn(merged, fallback_goal=previous.get("goal") or user_request)


def state_first_open_question(
    state: Optional[Dict[str, Any]],
    *,
    short_text_fn: Callable[..., str],
) -> str:
    source = state if isinstance(state, dict) else {}
    open_questions = source.get("open_questions")
    if isinstance(open_questions, list):
        for item in open_questions:
            line = short_text_fn(item, limit=220)
            if line:
                if line.endswith("?"):
                    return line
                return f"{line}?"
    return ""


def state_best_effort_answer(
    *,
    state: Optional[Dict[str, Any]],
    draft_response: str,
    tool_result: Optional[Dict[str, Any]],
    short_text_fn: Callable[..., str],
    is_low_information_text_fn: Callable[[Any], bool],
) -> str:
    draft = short_text_fn(draft_response, limit=320)
    if draft and not is_low_information_text_fn(draft):
        return draft
    source = state if isinstance(state, dict) else {}
    facts = source.get("facts") if isinstance(source.get("facts"), list) else []
    compact_facts = [short_text_fn(item, limit=180) for item in facts if short_text_fn(item, limit=180)]
    if compact_facts:
        return "; ".join(compact_facts[:3])
    summary = short_text_fn((tool_result or {}).get("summary_for_user"), limit=220)
    if summary and not is_low_information_text_fn(summary):
        return summary
    return "Completed."


def response_indicates_unfinished_work(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(i(?:'ll| will)\s+need\s+to|i(?:'m| am)\s+still\s+need(?:ing)?\s+to|still\s+need\s+to|not yet\b|need to (?:retrieve|fetch|get|look up|check)|haven't yet)\b",
            lowered,
        )
    )


def should_continue_after_incomplete_final_answer(
    *,
    user_text: str,
    final_text: str,
    retry_allowed: bool,
    contains_action_intent_fn: Callable[[str], bool],
    looks_like_weather_request_fn: Callable[[str], bool],
    looks_like_schedule_request_fn: Callable[[str], bool],
    looks_like_send_message_intent_fn: Callable[[str], bool],
    response_indicates_unfinished_work_fn: Callable[[str], bool] = response_indicates_unfinished_work,
) -> bool:
    if not retry_allowed:
        return False
    user_lowered = " ".join(str(user_text or "").strip().lower().split())
    actionable = bool(
        contains_action_intent_fn(user_lowered)
        or looks_like_weather_request_fn(user_lowered)
        or looks_like_schedule_request_fn(user_lowered)
        or looks_like_send_message_intent_fn(user_lowered)
    )
    if not actionable:
        return False
    return response_indicates_unfinished_work_fn(final_text)
