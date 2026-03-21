import json
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


def compact_tool_result_for_thanatos(
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


async def run_thanatos_state_update(
    *,
    llm_client: Any,
    platform: str,
    user_request: str,
    prior_state: Optional[Dict[str, Any]],
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    max_tokens: Optional[int],
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    configured_thanatos_max_tokens_fn: Callable[[], Optional[int]],
    coerce_text_fn: Callable[[Any], str],
    first_json_object_fn: Callable[[str], Optional[Dict[str, Any]]],
    state_add_line_fn: Callable[[List[str], str, int], List[str]],
    tool_history_line_fn: Callable[[Optional[Dict[str, Any]], Optional[Dict[str, Any]]], str],
    short_text_fn: Callable[..., str],
    is_low_information_text_fn: Callable[[Any], bool],
    state_list_fn: Callable[[Any, int, int], List[str]],
) -> Dict[str, Any]:
    previous = normalize_agent_state_fn(prior_state, fallback_goal=user_request)

    def _merge_lines(existing: List[str], incoming: Any, *, max_items: int, item_limit: int) -> List[str]:
        merged_lines = list(existing or [])
        for line in state_list_fn(incoming, max_items=max_items, item_limit=item_limit):
            merged_lines = state_add_line_fn(merged_lines, line, max_items)
        return merged_lines

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
        "tool_result": compact_tool_result_for_thanatos(tool_result, short_text_fn=short_text_fn),
    }
    prompt = (
        "You are Thanatos, the Reaper head in the Astraeus/Thanatos/Hermes loop.\n"
        "Update only the agent state.\n"
        "Return exactly one compact JSON object with keys:\n"
        "goal, plan, facts, open_questions, next_step, tool_history\n"
        "Rules:\n"
        "- Use short plain text snippets.\n"
        "- Do not invent facts; use only explicit evidence from payload.tool_result or prior_state.\n"
        "- Preserve prior successful facts and tool_history; do not drop earlier completed steps.\n"
        "- Keep plan as the remaining checklist of explicit user-requested actions.\n"
        "- For compound requests, plan must contain one item per explicit requested action.\n"
        "- If multiple actions were requested, keep unfinished items in plan and set next_step to the next unfinished action.\n"
        "- Remove plan items that are already completed.\n"
        "- A plan item is completed only when payload.tool_result provides direct evidence for that specific item.\n"
        "- Generic summaries (for example: 'Completed', 'Done', 'Completed N of N actions') are not enough to close information-seeking items.\n"
        "- For information requests, keep the item unfinished unless the concrete requested value is present in summary_for_user or data_preview.\n"
        "- Keep facts stable and deterministic.\n"
        "- When tool_result.ok is true, include one short fact describing what this specific step accomplished. Use payload.user_request when needed.\n"
        "- Record completion facts only when tool_result.ok is true; for failures, keep blocker details in open_questions.\n"
        "- Keep open_questions only for real blockers.\n"
        "- next_step is a short tool sketch or empty.\n"
        "- No markdown."
    )
    merged: Dict[str, Any] = dict(previous)
    try:
        token_limit = int(max_tokens) if max_tokens is not None else configured_thanatos_max_tokens_fn()
        chat_kwargs: Dict[str, Any] = {
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            "temperature": 0.1,
            "max_tokens": (max(1, int(token_limit)) if token_limit is not None else None),
        }
        response = await llm_client.chat(**chat_kwargs)
        text = coerce_text_fn((response.get("message", {}) or {}).get("content", "")).strip()
        patch_state = first_json_object_fn(text)
        if isinstance(patch_state, dict):
            patch = normalize_agent_state_fn(
                patch_state,
                fallback_goal=previous.get("goal") or user_request,
            )
            merged = dict(previous)
            merged["goal"] = patch.get("goal") or previous.get("goal") or str(user_request or "")
            merged["plan"] = list(patch.get("plan") or previous.get("plan") or [])
            merged["next_step"] = patch.get("next_step") or previous.get("next_step") or ""
            merged["open_questions"] = list(patch.get("open_questions") or previous.get("open_questions") or [])
            merged["facts"] = _merge_lines(
                list(previous.get("facts") or []),
                patch.get("facts"),
                max_items=8,
                item_limit=140,
            )
            merged["tool_history"] = _merge_lines(
                list(previous.get("tool_history") or []),
                patch.get("tool_history"),
                max_items=8,
                item_limit=150,
            )
            if isinstance(previous.get("plan_steps"), list):
                merged["plan_steps"] = list(previous.get("plan_steps") or [])
            if isinstance(previous.get("result_memory"), list):
                merged["result_memory"] = [dict(item) for item in previous.get("result_memory") if isinstance(item, dict)]
    except Exception:
        merged = dict(previous)

    merged["tool_history"] = state_add_line_fn(
        list(merged.get("tool_history") or []),
        tool_history_line_fn(tool_call, tool_result),
        8,
    )

    summary = short_text_fn((tool_result or {}).get("summary_for_user"), limit=150)
    if bool((tool_result or {}).get("ok")) and summary and not is_low_information_text_fn(summary):
        merged["facts"] = state_add_line_fn(list(merged.get("facts") or []), summary, 8)
    elif bool((tool_result or {}).get("ok")):
        completed_step = short_text_fn(f"Completed: {user_request}", limit=150)
        if completed_step:
            merged["facts"] = state_add_line_fn(list(merged.get("facts") or []), completed_step, 8)

    if not bool((tool_result or {}).get("ok")):
        errors = (tool_result or {}).get("errors")
        if isinstance(errors, list) and errors:
            merged["open_questions"] = state_add_line_fn(
                list(merged.get("open_questions") or []),
                short_text_fn(errors[0], limit=150),
                4,
            )
    else:
        merged["open_questions"] = []

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
