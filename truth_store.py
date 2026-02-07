import json
import time
from typing import Any, Dict


def save_truth_snapshot(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    plugin_id: str,
    truth: Dict[str, Any],
    max_entries: int = 50,
) -> None:
    if redis_client is None:
        return

    entry = {
        "ts": time.time(),
        "platform": platform,
        "scope": scope,
        "plugin_id": plugin_id,
        "truth": truth,
    }
    payload = json.dumps(entry, ensure_ascii=False)
    list_key = f"tater:truth:{platform}:{scope}"
    latest_key = f"tater:truth:last:{platform}:{scope}"

    try:
        redis_client.rpush(list_key, payload)
        if max_entries > 0:
            redis_client.ltrim(list_key, -max_entries, -1)
        redis_client.set(latest_key, payload)
    except Exception:
        return

