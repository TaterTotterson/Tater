import re
from typing import Any, Callable, Dict, List, Optional

from plugin_kernel import plugin_supports_platform

MULTI_INTENT_ROUTE_USER_TEXT_KEY = "__user_text"
MULTI_INTENT_ROUTE_FLAG_KEY = "__routed_multi_intent"
_MULTI_INTENT_SPLIT_VERBS = (
    "turn",
    "set",
    "tell",
    "show",
    "get",
    "give",
    "send",
    "play",
    "open",
    "search",
    "check",
    "list",
    "run",
    "create",
    "add",
    "remove",
    "delete",
    "summarize",
    "draw",
    "post",
    "message",
    "dm",
    "notify",
    "remind",
    "schedule",
    "start",
    "stop",
    "restart",
    "reboot",
    "fetch",
    "find",
    "read",
    "write",
    "update",
)
_MULTI_INTENT_CLAUSE_SPLIT_RE = re.compile(
    r"\s*(?:;|\n+)\s*"
    r"|\s*(?:,\s*)?(?:and\s+then|then|also|plus|as\s+well\s+as|in\s+addition\s+to)\s+"
    r"|\s+\band\s+(?=(?:"
    + "|".join(_MULTI_INTENT_SPLIT_VERBS)
    + r")\b)",
    flags=re.IGNORECASE,
)


def plugin_routing_keywords(plugin: Any) -> List[str]:
    raw = getattr(plugin, "routing_keywords", [])
    if raw is None:
        return []
    if isinstance(raw, str):
        candidates = [raw]
    elif isinstance(raw, (list, tuple, set)):
        candidates = list(raw)
    else:
        return []

    out: List[str] = []
    seen: set[str] = set()
    for item in candidates:
        normalized = " ".join(str(item or "").strip().lower().split())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def tool_call_route_metadata(tool_call: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    source = tool_call if isinstance(tool_call, dict) else {}
    out: Dict[str, Any] = {}
    routed_user_text = source.get(MULTI_INTENT_ROUTE_USER_TEXT_KEY)
    if isinstance(routed_user_text, str) and routed_user_text.strip():
        out[MULTI_INTENT_ROUTE_USER_TEXT_KEY] = routed_user_text
    if bool(source.get(MULTI_INTENT_ROUTE_FLAG_KEY)):
        out[MULTI_INTENT_ROUTE_FLAG_KEY] = True
    return out


def tool_call_effective_user_text(tool_call: Optional[Dict[str, Any]], default_user_text: str) -> str:
    source = tool_call if isinstance(tool_call, dict) else {}
    routed_user_text = source.get(MULTI_INTENT_ROUTE_USER_TEXT_KEY)
    if isinstance(routed_user_text, str) and routed_user_text.strip():
        return routed_user_text
    return str(default_user_text or "")


def compose_multi_intent_route_answer(summaries: List[str], fallback: str = "") -> str:
    cleaned: List[str] = []
    seen: set[str] = set()
    for item in summaries or []:
        text = " ".join(str(item or "").strip().split())
        if not text:
            continue
        token = text.lower()
        if token in seen:
            continue
        seen.add(token)
        cleaned.append(text)
    if not cleaned:
        return str(fallback or "").strip()
    return " ".join(cleaned).strip()


def is_routed_multi_intent_tool_call(tool_call: Optional[Dict[str, Any]]) -> bool:
    source = tool_call if isinstance(tool_call, dict) else {}
    return bool(source.get(MULTI_INTENT_ROUTE_FLAG_KEY))


def split_multi_intent_action_clauses(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    parts = [chunk.strip(" \t\r\n,;") for chunk in _MULTI_INTENT_CLAUSE_SPLIT_RE.split(raw) if chunk and chunk.strip()]
    if not parts:
        return [raw]

    out: List[str] = []
    for part in parts:
        cleaned = re.sub(r"^(?:and|then|also|plus)\b[\s,:-]*", "", part, flags=re.IGNORECASE).strip(" \t\r\n,;")
        if cleaned:
            out.append(cleaned)
    return out or [raw]


def score_clause_for_plugin_keywords(clause_text: str, keywords: List[str]) -> tuple[int, int]:
    normalized_clause = " " + " ".join(str(clause_text or "").strip().lower().split()) + " "
    if len(normalized_clause.strip()) < 3:
        return 0, 0

    score = 0
    longest_match = 0
    for keyword in keywords:
        token = " ".join(str(keyword or "").strip().lower().split())
        if not token:
            continue
        if " " in token:
            if token in normalized_clause:
                score += max(2, len(token.split()))
                longest_match = max(longest_match, len(token))
            continue
        if re.search(rf"\b{re.escape(token)}\b", normalized_clause, flags=re.IGNORECASE):
            score += 1
            longest_match = max(longest_match, len(token))
    return score, longest_match


def route_clause_to_plugin(
    *,
    clause_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    normalized_clause = " ".join(str(clause_text or "").strip().split())
    if not normalized_clause:
        return ""

    best_plugin_id = ""
    best_score = 0
    best_longest_match = 0
    for plugin_id, plugin_obj in sorted((registry or {}).items(), key=lambda item: str(item[0] or "")):
        if not plugin_id or plugin_obj is None:
            continue
        if enabled_predicate and not enabled_predicate(plugin_id):
            continue
        if not plugin_supports_platform(plugin_obj, platform):
            continue
        keywords = plugin_routing_keywords(plugin_obj)
        if not keywords:
            continue
        score, longest_match = score_clause_for_plugin_keywords(normalized_clause, keywords)
        if score <= 0:
            continue
        if score > best_score or (score == best_score and longest_match > best_longest_match):
            best_plugin_id = str(plugin_id)
            best_score = score
            best_longest_match = longest_match
    return best_plugin_id if best_score > 0 else ""


def build_multi_intent_routed_actions(
    *,
    request_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> List[Dict[str, str]]:
    clauses = split_multi_intent_action_clauses(request_text)
    if len(clauses) < 2:
        return []

    routed_pairs: List[tuple[str, str]] = []
    for clause in clauses:
        plugin_id = route_clause_to_plugin(
            clause_text=clause,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )
        if not plugin_id:
            return []
        routed_pairs.append((plugin_id, clause))

    distinct_plugins = {plugin_id for plugin_id, _clause in routed_pairs}
    if len(distinct_plugins) < 2:
        return []

    per_plugin_clauses: Dict[str, List[str]] = {}
    plugin_order: List[str] = []
    for plugin_id, clause in routed_pairs:
        if plugin_id not in per_plugin_clauses:
            per_plugin_clauses[plugin_id] = []
            plugin_order.append(plugin_id)
        per_plugin_clauses[plugin_id].append(clause)

    out: List[Dict[str, str]] = []
    for plugin_id in plugin_order:
        snippets = per_plugin_clauses.get(plugin_id) or []
        if not snippets:
            continue
        snippet = " and ".join(piece for piece in snippets if str(piece or "").strip()).strip()
        if not snippet:
            continue
        out.append({"plugin_id": plugin_id, "user_text": snippet})
    return out if len(out) >= 2 else []
