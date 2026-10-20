import hashlib
import json
import time
from typing import Any, Dict, Iterable, Optional


PENDING_INTERACTION_TTL_SECONDS = 30 * 60
PENDING_INTERACTION_KEY_PREFIX = "tater:hydra:pending_interaction:"
PENDING_INTERACTION_KINDS = {"action_offer", "clarification"}
PENDING_FOLLOWUP_RELATIONS = {"confirm", "provide_details", "modify"}


def _short(value: Any, *, limit: int) -> str:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def _text_list(value: Any, *, max_items: int, item_limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    rows: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = _short(item, limit=item_limit)
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        rows.append(text)
        if len(rows) >= max_items:
            break
    return rows


def normalize_pending_interaction(
    value: Any,
    *,
    available_tool_ids: Optional[Iterable[str]] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    kind = str(value.get("kind") or "").strip().lower()
    if kind not in PENDING_INTERACTION_KINDS:
        return None

    allowed_was_supplied = available_tool_ids is not None
    allowed = {
        str(item or "").strip()
        for item in (available_tool_ids or [])
        if str(item or "").strip()
    }
    tool_hint = _short(value.get("tool_hint"), limit=80)
    if not tool_hint or (allowed_was_supplied and tool_hint not in allowed):
        return None

    goal = _short(value.get("goal"), limit=240)
    if not goal:
        return None

    normalized: Dict[str, Any] = {
        "kind": kind,
        "goal": goal,
        "tool_hint": tool_hint,
        "known_details": _text_list(value.get("known_details"), max_items=12, item_limit=180),
        "missing_details": _text_list(value.get("missing_details"), max_items=8, item_limit=120),
        "choices": _text_list(value.get("choices"), max_items=12, item_limit=120),
        "question": _short(value.get("question"), limit=320),
    }
    source_turn_id = _short(value.get("source_turn_id"), limit=80)
    if source_turn_id:
        normalized["source_turn_id"] = source_turn_id
    try:
        created_at = float(value.get("created_at") or 0.0)
    except Exception:
        created_at = 0.0
    if created_at > 0:
        normalized["created_at"] = created_at
    return normalized


def pending_interaction_key(*, platform: str, scope: str) -> str:
    platform_token = _short(platform, limit=48).lower() or "unknown"
    scope_token = str(scope or "").strip() or "default"
    scope_hash = hashlib.sha256(scope_token.encode("utf-8", errors="ignore")).hexdigest()[:24]
    return f"{PENDING_INTERACTION_KEY_PREFIX}{platform_token}:{scope_hash}"


def load_pending_interaction(
    redis_client: Any,
    *,
    platform: str,
    scope: str,
    available_tool_ids: Optional[Iterable[str]] = None,
) -> Optional[Dict[str, Any]]:
    if redis_client is None:
        return None
    try:
        raw = redis_client.get(pending_interaction_key(platform=platform, scope=scope))
    except Exception:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    try:
        parsed = json.loads(str(raw or ""))
    except Exception:
        return None
    return normalize_pending_interaction(parsed, available_tool_ids=available_tool_ids)


def save_pending_interaction(
    redis_client: Any,
    *,
    platform: str,
    scope: str,
    interaction: Any,
    available_tool_ids: Optional[Iterable[str]] = None,
    source_turn_id: str = "",
    ttl_seconds: int = PENDING_INTERACTION_TTL_SECONDS,
) -> Optional[Dict[str, Any]]:
    normalized = normalize_pending_interaction(
        interaction,
        available_tool_ids=available_tool_ids,
    )
    if redis_client is None or normalized is None:
        return None
    if source_turn_id:
        normalized["source_turn_id"] = _short(source_turn_id, limit=80)
    normalized.setdefault("created_at", time.time())
    try:
        payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), default=str)
        redis_client.set(
            pending_interaction_key(platform=platform, scope=scope),
            payload,
            ex=max(60, int(ttl_seconds or PENDING_INTERACTION_TTL_SECONDS)),
        )
    except Exception:
        return None
    return normalized


def clear_pending_interaction(redis_client: Any, *, platform: str, scope: str) -> None:
    if redis_client is None:
        return
    try:
        redis_client.delete(pending_interaction_key(platform=platform, scope=scope))
    except Exception:
        return


def clarification_text(interaction: Any, *, preferred: str = "") -> str:
    preferred_text = _short(preferred, limit=320)
    if preferred_text:
        return preferred_text
    source = interaction if isinstance(interaction, dict) else {}
    choices = _text_list(source.get("choices"), max_items=8, item_limit=120)
    missing = _text_list(source.get("missing_details"), max_items=4, item_limit=120)
    if choices:
        if len(choices) == 1:
            choice_text = choices[0]
        elif len(choices) == 2:
            choice_text = f"{choices[0]} or {choices[1]}"
        else:
            choice_text = ", ".join(choices[:-1]) + f", or {choices[-1]}"
        subject = missing[0] if missing else "option"
        return f"Which {subject} would you like: {choice_text}?"
    if missing:
        return f"What should I use for {missing[0]}?"
    question = _short(source.get("question"), limit=320)
    if question:
        return question
    return "What detail should I use before I do that?"


def resolve_turn_branch(
    *,
    planned_mode: str,
    relation: str,
    has_executable_steps: bool,
    current_interaction: Any,
    next_interaction: Any,
) -> tuple[str, Optional[Dict[str, Any]]]:
    mode = str(planned_mode or "").strip().lower()
    relation_value = str(relation or "").strip().lower()
    current = current_interaction if isinstance(current_interaction, dict) else None
    updated = next_interaction if isinstance(next_interaction, dict) else None

    if has_executable_steps:
        return "execute", None
    if current is not None and relation_value == "reject":
        return "reject", None
    if current is not None and relation_value in PENDING_FOLLOWUP_RELATIONS:
        return "clarify", updated or current
    if mode == "clarify":
        if updated is not None:
            return "clarify", updated
        return "blocked", None
    if mode == "reject":
        return "reject", None
    if mode == "execute":
        return "blocked", None
    return "chat", None
