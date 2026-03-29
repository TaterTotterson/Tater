import re
from typing import Any, Callable, Dict, List, Optional


def state_list(
    value: Any,
    *,
    max_items: int,
    item_limit: int,
    coerce_text_fn: Callable[[Any], str],
    short_text_fn: Callable[..., str],
) -> List[str]:
    items: List[Any]
    if isinstance(value, list):
        items = value
    elif isinstance(value, str) and value.strip():
        items = [value]
    else:
        items = []
    out: List[str] = []
    for item in items:
        line = " ".join(coerce_text_fn(item).split())
        line = short_text_fn(line, limit=item_limit)
        if not line:
            continue
        out.append(line)
        if len(out) >= max_items:
            break
    return out


def state_next_step(
    value: Any,
    *,
    coerce_text_fn: Callable[[Any], str],
    short_text_fn: Callable[..., str],
) -> str:
    if isinstance(value, dict):
        func = str(value.get("function") or "").strip()
        args = value.get("arguments") if isinstance(value.get("arguments"), dict) else {}
        if not func:
            return ""
        args_hint: List[str] = []
        for key in list(args.keys())[:2]:
            val = args.get(key)
            text = short_text_fn(coerce_text_fn(val), limit=40)
            if text:
                args_hint.append(f"{key}={text}")
        if args_hint:
            return short_text_fn(f"{func}({', '.join(args_hint)})", limit=180)
        return short_text_fn(func, limit=180)
    return short_text_fn(" ".join(coerce_text_fn(value).split()), limit=180)


def normalize_agent_state(
    state: Optional[Dict[str, Any]],
    *,
    fallback_goal: str,
    coerce_text_fn: Callable[[Any], str],
    short_text_fn: Callable[..., str],
    state_list_fn: Callable[..., list[str]],
    state_next_step_fn: Callable[[Any], str],
) -> Dict[str, Any]:
    source = state if isinstance(state, dict) else {}
    goal = short_text_fn(" ".join(coerce_text_fn(source.get("goal")).split()), limit=180)
    if not goal:
        goal = short_text_fn(" ".join(str(fallback_goal or "").split()), limit=180) or "Fulfill the user request."
    out = {
        "goal": goal,
        "plan": state_list_fn(source.get("plan"), max_items=8, item_limit=140),
        "facts": state_list_fn(source.get("facts"), max_items=8, item_limit=140),
        "open_questions": state_list_fn(source.get("open_questions"), max_items=4, item_limit=160),
        "next_step": state_next_step_fn(source.get("next_step")),
        "tool_history": state_list_fn(source.get("tool_history"), max_items=8, item_limit=150),
    }
    return out


def references_previous_work(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(it|that|this|those|them|again|continue|same|still|as before|last one|previous|earlier|above)\b",
            lowered,
        )
    )


def looks_like_short_followup_request(
    text: str,
    *,
    references_previous_work_fn: Callable[[str], bool],
) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    tokens = [tok for tok in lowered.split(" ") if tok]
    if len(tokens) > 3:
        return False
    if references_previous_work_fn(lowered):
        return True
    if lowered in {"ok", "okay", "do it", "go ahead", "again", "same", "same thing"}:
        return True
    return bool(re.search(r"^(ok|okay|yes|yep|sure)\b", lowered))


def contains_new_domain_reset_keywords(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(weather|download|summarize|summary|reminder|task|search web|inspect webpage|memory|upload|send message)\b",
            lowered,
        )
    )


def references_explicit_prior_work(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(it|that|again|previous|as before|same as before)\b",
            lowered,
        )
    )


def should_reset_state_for_topic_change(
    current_user_text: str,
    *,
    contains_new_domain_reset_keywords_fn: Callable[[str], bool],
    references_explicit_prior_work_fn: Callable[[str], bool],
    looks_like_short_followup_request_fn: Callable[[str], bool],
    references_previous_work_fn: Callable[[str], bool],
    looks_like_standalone_request_fn: Callable[[str], bool],
) -> bool:
    current = str(current_user_text or "").strip()
    if not current:
        return False
    if contains_new_domain_reset_keywords_fn(current):
        if references_explicit_prior_work_fn(current):
            return False
        return True
    if looks_like_short_followup_request_fn(current):
        return False
    if references_previous_work_fn(current):
        return False
    if not looks_like_standalone_request_fn(current):
        return False
    return True


def new_agent_state(
    goal: str,
    *,
    normalize_agent_state_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    return normalize_agent_state_fn(
        {
            "goal": goal,
            "plan": [],
            "facts": [],
            "open_questions": [],
            "next_step": "",
            "tool_history": [],
        },
        fallback_goal=goal,
    )


def initial_agent_state_for_turn(
    *,
    prior_state: Optional[Dict[str, Any]],
    current_user_text: str,
    turn_request_text: str,
    short_text_fn: Callable[..., str],
    should_reset_state_for_topic_change_fn: Callable[[str], bool],
    new_agent_state_fn: Callable[[str], Dict[str, Any]],
    normalize_agent_state_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    goal = short_text_fn((turn_request_text or current_user_text or "").strip(), limit=180)
    goal = goal or "Fulfill the user request."
    if not isinstance(prior_state, dict):
        return new_agent_state_fn(goal)
    if should_reset_state_for_topic_change_fn(current_user_text):
        return new_agent_state_fn(goal)
    merged = normalize_agent_state_fn(prior_state, fallback_goal=goal)
    merged["goal"] = goal
    return merged
