import re
from typing import Callable, Pattern


def contains_action_intent(
    text: str,
    *,
    url_re: Pattern[str],
) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if url_re.search(lowered):
        return True
    return bool(
        re.search(
            r"\b(can|could|would|will|please|make|build|create|turn|set|check|find|search|summarize|download|upload|send|post|add|share|inspect|read|run|open|explain|help)\b",
            lowered,
        )
    )


def is_acknowledgement_only(
    text: str,
    *,
    contains_action_intent_fn: Callable[[str], bool],
) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    if contains_action_intent_fn(lowered):
        return False
    if "?" in lowered:
        return False
    if re.search(
        r"\b(thanks|thank you|thx|ty|appreciate it|got it|sounds good|all good|perfect|awesome|great|cool|nice)\b",
        lowered,
    ):
        return True
    return False


def is_stop_only(
    text: str,
    *,
    contains_action_intent_fn: Callable[[str], bool],
) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    if re.search(r"\b(do not|don't)\s+(send|post|add|upload|share|continue|keep going|go on)\b", lowered):
        return True
    if re.search(r"\b(stop|cancel|nevermind|never mind|do not|don't)\b", lowered):
        if contains_action_intent_fn(lowered):
            # Allow explicit cancels even with a trailing polite phrase, but avoid
            # swallowing new actionable requests.
            if re.search(r"\b(stop|cancel|nevermind|never mind)\b", lowered):
                return True
            return False
        return True
    if lowered in {"no", "nope", "nah", "no thanks", "that's all", "thats all"}:
        return True
    return False


def is_casual_greeting_only(
    text: str,
    *,
    contains_action_intent_fn: Callable[[str], bool],
    url_re: Pattern[str],
    references_previous_work_fn: Callable[[str], bool],
    looks_like_schedule_request_fn: Callable[[str], bool],
    looks_like_weather_request_fn: Callable[[str], bool],
    looks_like_send_message_intent_fn: Callable[[str], bool],
) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    if contains_action_intent_fn(lowered):
        return False
    if url_re.search(lowered):
        return False
    if references_previous_work_fn(lowered):
        return False
    if looks_like_schedule_request_fn(lowered) or looks_like_weather_request_fn(lowered):
        return False
    if looks_like_send_message_intent_fn(lowered):
        return False

    normalized = re.sub(r"[^\w\s']", " ", lowered)
    normalized = " ".join(normalized.split())
    if not normalized:
        return False
    tokens = [tok for tok in normalized.split(" ") if tok]
    if len(tokens) > 6:
        return False
    if re.fullmatch(r"(?:hey|hi|hello|yo|hiya|howdy)(?:\s+\w+){0,4}", normalized):
        return True
    if re.fullmatch(r"(?:good morning|good afternoon|good evening)(?:\s+\w+){0,3}", normalized):
        return True
    if re.fullmatch(r"(?:how are you|what'?s up|whats up)(?:\s+\w+){0,3}", normalized):
        return True
    return False
