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
