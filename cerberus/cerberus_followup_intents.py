import re
from typing import Callable, Pattern


def looks_like_standalone_request(
    text: str,
    *,
    is_acknowledgement_only_fn: Callable[[str], bool],
    is_stop_only_fn: Callable[[str], bool],
    url_re: Pattern[str],
) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    lower = raw.lower()
    if re.match(r"^(?:what|how)\s+about\b", lower):
        return False
    if is_acknowledgement_only_fn(lower) or is_stop_only_fn(lower):
        return False
    if url_re.search(raw):
        return True
    if "?" in raw:
        return True
    if re.match(
        r"^(?:hey|hi|hello|please|can|could|will|would|what|who|where|when|why|how|tell|show|describe|explain)\b",
        lower,
    ):
        return True
    if any(phrase in lower for phrase in ("can you", "could you", "will you", "would you", "help me", "i need ")):
        return True
    return False


def looks_like_short_followup(
    text: str,
    *,
    is_acknowledgement_only_fn: Callable[[str], bool],
    is_stop_only_fn: Callable[[str], bool],
    looks_like_standalone_request_fn: Callable[[str], bool],
) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if re.match(r"^(?:what|how)\s+about\b", lowered):
        tokens = re.findall(r"[a-z0-9']+", lowered)
        return 2 <= len(tokens) <= 7
    if is_acknowledgement_only_fn(lowered) or is_stop_only_fn(lowered):
        return False
    if looks_like_standalone_request_fn(lowered):
        return False
    if lowered in {
        "yes",
        "no",
        "yep",
        "nope",
        "do it",
        "do that",
        "go ahead",
        "check",
        "check state",
        "state",
        "same",
        "again",
        "retry",
        "on",
        "off",
    }:
        return True
    tokens = re.findall(r"[a-z0-9']+", lowered)
    if not tokens or len(tokens) > 6:
        return False
    referential = {"it", "that", "this", "them", "those", "same"}
    return any(tok in referential for tok in tokens)


def looks_like_download_followup(
    text: str,
    *,
    url_re: Pattern[str],
) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if url_re.search(lowered):
        return False
    has_action = bool(
        re.search(
            r"\b(download|save|get|grab|fetch|retrieve|pull|attach|post|send|share)\b",
            lowered,
        )
    )
    if not has_action:
        return False
    has_ref = bool(
        re.search(
            r"\b(link|url|file|image|photo|video|audio|zip|document|pdf|it|that|this|them|those)\b",
            lowered,
        )
    )
    return has_ref


def looks_like_send_message_intent(
    text: str,
    *,
    url_re: Pattern[str],
) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if url_re.search(lowered):
        return False
    has_send_verb = bool(re.search(r"\b(send|dm|post|share|forward)\b", lowered))
    has_message_verb = bool(re.search(r"\bmessage\s+(it|this|that|them|those|here)\b", lowered))
    if not has_send_verb and not has_message_verb:
        return False
    if re.search(r"\b(explain|explanation|format|what does|what is)\b", lowered):
        return False
    has_ref = bool(
        re.search(
            r"\b(link|url|file|image|photo|video|audio|zip|document|pdf|it|that|this|them|those|here|there|channel|room|chat|discord|irc|matrix|telegram|homeassistant|home assistant)\b",
            lowered,
        )
    )
    return has_ref


def looks_like_link_list_request(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(?:show|send|give|list)\s+(?:me\s+)?(?:just\s+|only\s+)?(?:links?|urls?|sources?|sites?|websites?)\b"
            r"|(?:\bjust\s+|only\s+)(?:links?|urls?|sources?|sites?|websites?)\b"
            r"|\btop\s+\d+\s+(?:links?|sites?|websites?)\b",
            lowered,
        )
    )
