import re
from typing import Callable, Pattern, Sequence


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
            r"\b(can|could|would|please|make|build|create|do|turn|set|check|find|search|summarize|download|upload|send|post|add|share|inspect|read|run|open|explain|help)\b",
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


def looks_like_over_clarification(
    text: str,
    *,
    user_text: str,
    over_clarification_markers: Sequence[str],
    looks_like_weather_request_fn: Callable[[str], bool],
    looks_like_schedule_request_fn: Callable[[str], bool],
    contains_action_intent_fn: Callable[[str], bool],
) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if "?" not in lowered:
        return False
    marker_hit = any(marker in lowered for marker in over_clarification_markers)
    generic_clarifier_hit = bool(
        re.search(
            r"\b(platform|environment|room|channel|chat|time\s*zone|timezone|time format|12-hour|24-hour|am or pm|city|location|coordinates?|zip|postal)\b",
            lowered,
        )
    )
    if not marker_hit and not generic_clarifier_hit:
        return False

    user_lowered = " ".join(str(user_text or "").strip().lower().split())
    if not user_lowered:
        return False

    if re.search(r"\b(platform|environment)\b", lowered):
        return bool(
            re.search(
                r"\b(discord|irc|matrix|telegram|homeassistant|home assistant|homekit|xbmc|webui|here|this chat|this channel|this room)\b",
                user_lowered,
            )
        )

    if re.search(r"\b(room|channel|chat|where should i send|which .* send)\b", lowered):
        return bool(
            re.search(
                r"\b(here|this chat|this channel|this room|in this|to discord|to irc|to matrix|to telegram|to homeassistant|to home assistant|to homekit|to xbmc|channel|room|chat|dm)\b",
                user_lowered,
            )
        )

    if re.search(r"\b(time format|12-hour|24-hour|am or pm|a\.m\.|p\.m\.)\b", lowered):
        return bool(
            re.search(
                r"\b(\d{1,2}(?::\d{2})?\s*(am|pm)?|every day|everyday|daily|weekly|tomorrow|today|at\s+\d{1,2})\b",
                user_lowered,
            )
        )

    if re.search(r"\b(timezone|time zone|iana|utc|gmt)\b", lowered):
        has_time = bool(
            re.search(
                r"\b(\d{1,2}(?::\d{2})?\s*(am|pm)?|at\s+\d{1,2}(?::\d{2})?\s*(am|pm)?|morning|afternoon|evening|night)\b",
                user_lowered,
            )
        )
        has_schedule = bool(
            re.search(
                r"\b(remind|reminder|schedule|scheduled|task|timer|alarm|forecast|every day|everyday|daily|weekly|weekdays?|weekends?)\b",
                user_lowered,
            )
        )
        return bool(has_time and has_schedule)

    if re.search(r"\b(city|coordinates?|location|zip|postal)\b", lowered):
        return bool(looks_like_weather_request_fn(user_lowered) or looks_like_schedule_request_fn(user_lowered))

    if re.search(r"\b(what would you like me to do|what would you like to do)\b", lowered):
        return contains_action_intent_fn(user_lowered)

    if re.search(r"\b(which|what)\b.{0,90}\b(categories?|channels?|roles?)\b", lowered):
        return bool(
            re.search(
                r"\b(discord|server|setup|set up|configure|build|hq|for us|however\s+you\s+think)\b",
                user_lowered,
            )
        )

    if re.search(r"\b(could you clarify|what do you mean|what specific issue)\b", lowered):
        tokens = [tok for tok in re.split(r"\s+", user_lowered) if tok]
        if len(tokens) <= 2 and not contains_action_intent_fn(user_lowered):
            return False
        return True

    return False
