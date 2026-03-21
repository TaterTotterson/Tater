import re
from typing import Any, Callable, Optional


def coerce_non_negative_int(value: Any, default: int) -> int:
    candidate: Any = value
    if isinstance(candidate, (bytes, bytearray)):
        try:
            candidate = candidate.decode("utf-8", errors="ignore")
        except Exception:
            candidate = ""
    try:
        out = int(str(candidate).strip())
    except Exception:
        out = int(default)
    if out < 0:
        return 0
    return out


def resolve_agent_limits(
    redis_client: Any = None,
    *,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
    fallback_redis: Any,
    coerce_non_negative_int_fn: Callable[[Any, int], int],
    default_max_rounds: int,
    default_max_tool_calls: int,
    agent_max_rounds_key: str,
    agent_max_tool_calls_key: str,
) -> tuple[int, int]:
    redis_ref = redis_client or fallback_redis
    stored_rounds = default_max_rounds
    stored_tool_calls = default_max_tool_calls
    try:
        stored_rounds = coerce_non_negative_int_fn(
            redis_ref.get(agent_max_rounds_key),
            default_max_rounds,
        )
    except Exception:
        stored_rounds = default_max_rounds
    try:
        stored_tool_calls = coerce_non_negative_int_fn(
            redis_ref.get(agent_max_tool_calls_key),
            default_max_tool_calls,
        )
    except Exception:
        stored_tool_calls = default_max_tool_calls

    effective_rounds = (
        stored_rounds
        if max_rounds is None
        else coerce_non_negative_int_fn(max_rounds, stored_rounds)
    )
    effective_tool_calls = (
        stored_tool_calls
        if max_tool_calls is None
        else coerce_non_negative_int_fn(max_tool_calls, stored_tool_calls)
    )
    return effective_rounds, effective_tool_calls


def estimated_requested_action_count(
    text: str,
    *,
    max_budget: int,
) -> int:
    normalized = " ".join(str(text or "").replace("&", " and ").strip().lower().split())
    if not normalized:
        return 1
    connector_hits = len(
        re.findall(
            r"\b(?:and then|and also|as well as|plus|also|then|along with|in addition to)\b",
            normalized,
            flags=re.IGNORECASE,
        )
    )
    connector_hits += len(
        re.findall(
            r"\band\s+(?:turn|set|tell|show|get|give|send|play|open|search|check|list|run|create|add|remove|delete|summarize|draw|post|message|dm|notify|remind|schedule|start|stop|restart|reboot|fetch|find|read|write|update)\b",
            normalized,
            flags=re.IGNORECASE,
        )
    )
    comma_hits = len(re.findall(r",\s*(?:and|then|also)\b", normalized, flags=re.IGNORECASE))
    clause_hits = len([part for part in re.split(r"\s*[;]\s*", normalized) if part.strip()])
    estimated = 1 + connector_hits + comma_hits
    if clause_hits > 1:
        estimated = max(estimated, clause_hits)
    return max(1, min(max_budget, estimated))


def expand_limits_for_compound_request(
    *,
    max_rounds: int,
    max_tool_calls: int,
    request_text: str,
    estimated_requested_action_count_fn: Callable[[str], int],
    min_budget: int,
    max_budget: int,
) -> tuple[int, int]:
    estimated_actions = estimated_requested_action_count_fn(request_text)
    if estimated_actions <= 1:
        return max_rounds, max_tool_calls
    target_budget = min(
        max_budget,
        max(min_budget, estimated_actions + 1),
    )
    expanded_rounds = max_rounds
    expanded_tool_calls = max_tool_calls
    if expanded_rounds > 0:
        expanded_rounds = max(expanded_rounds, target_budget)
    if expanded_tool_calls > 0:
        expanded_tool_calls = max(expanded_tool_calls, target_budget)
    return expanded_rounds, expanded_tool_calls
