import hashlib
import json
from typing import Any, Callable, Dict, Optional


def compact_agent_state_json(
    state: Optional[Dict[str, Any]],
    *,
    fallback_goal: str,
    limit: int,
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    short_text_fn: Callable[..., str],
) -> str:
    del limit, short_text_fn
    full_state = normalize_agent_state_fn(state, fallback_goal=fallback_goal)
    try:
        return json.dumps(full_state, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return "{}"


def agent_state_prompt_message(
    state: Optional[Dict[str, Any]],
    *,
    fallback_goal: str,
    prompt_max_chars: int,
    compact_agent_state_json_fn: Callable[..., str] = compact_agent_state_json,
) -> str:
    payload = compact_agent_state_json_fn(
        state,
        fallback_goal=fallback_goal,
        limit=prompt_max_chars,
    )
    return "Current agent state (full JSON):\n" + payload


def agent_state_hash(
    state: Optional[Dict[str, Any]],
    *,
    fallback_goal: str,
    ledger_max_chars: int,
    compact_agent_state_json_fn: Callable[..., str] = compact_agent_state_json,
) -> str:
    payload = compact_agent_state_json_fn(
        state,
        fallback_goal=fallback_goal,
        limit=ledger_max_chars,
    )
    digest = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
    return f"sha256:{digest}"
