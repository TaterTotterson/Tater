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
