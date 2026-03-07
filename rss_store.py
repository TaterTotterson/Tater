import json
import time
from typing import Any, Dict

FEEDS_KEY = "rss:feeds"


def _parse_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "on", "enabled"):
        return True
    if s in ("0", "false", "no", "off", "disabled"):
        return False
    return default


def _clean_targets(targets: Any) -> Dict[str, Any]:
    if not isinstance(targets, dict):
        return {}
    return {k: v for k, v in targets.items() if v not in (None, "")}


def _normalize_platforms(raw: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, val in raw.items():
        if not isinstance(val, dict):
            continue
        enabled = _parse_bool(val.get("enabled"), True)
        targets = _clean_targets(val.get("targets"))
        normalized[key] = {"enabled": enabled, "targets": targets}
    return normalized


def normalize_feed_config(raw: Any) -> Dict[str, Any]:
    """
    Normalize legacy entries into a consistent structure.

    Returns:
      {"last_ts": float, "enabled": bool, "portals": {..}}
    """
    data: Dict[str, Any]
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str):
        raw_s = raw.strip()
        if not raw_s:
            data = {}
        else:
            try:
                data = json.loads(raw_s)
                if not isinstance(data, dict):
                    data = {}
            except Exception:
                # legacy numeric timestamp
                try:
                    last_ts = float(raw_s)
                except Exception:
                    last_ts = 0.0
                return {
                    "last_ts": float(last_ts),
                    "enabled": True,
                    "portals": {},
                }
    else:
        data = {}

    last_ts = data.get("last_ts")
    try:
        last_ts = float(last_ts) if last_ts is not None else 0.0
    except Exception:
        last_ts = 0.0

    enabled = _parse_bool(data.get("enabled"), True)
    platforms = _normalize_platforms(data.get("portals"))

    return {
        "last_ts": float(last_ts),
        "enabled": enabled,
        "portals": platforms,
    }


def serialize_feed_config(config: Dict[str, Any]) -> str:
    cfg = normalize_feed_config(config)
    return json.dumps(cfg, ensure_ascii=False)


def get_all_feeds(redis_client) -> Dict[str, Dict[str, Any]]:
    raw = redis_client.hgetall(FEEDS_KEY) or {}
    out: Dict[str, Dict[str, Any]] = {}
    for feed_url, value in raw.items():
        out[str(feed_url)] = normalize_feed_config(value)
    return out


def get_feed(redis_client, feed_url: str) -> Dict[str, Any] | None:
    raw = redis_client.hget(FEEDS_KEY, feed_url)
    if raw is None:
        return None
    return normalize_feed_config(raw)


def set_feed(redis_client, feed_url: str, config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = normalize_feed_config(config)
    redis_client.hset(FEEDS_KEY, feed_url, serialize_feed_config(cfg))
    return cfg


def update_feed(redis_client, feed_url: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    current = get_feed(redis_client, feed_url) or {
        "last_ts": 0.0,
        "enabled": True,
        "portals": {},
    }

    next_cfg = dict(current)
    for key in ("last_ts", "enabled"):
        if key in updates:
            next_cfg[key] = updates.get(key)

    if "portals" in updates:
        next_cfg["portals"] = updates.get("portals") or {}

    return set_feed(redis_client, feed_url, next_cfg)


def delete_feed(redis_client, feed_url: str) -> bool:
    return bool(redis_client.hdel(FEEDS_KEY, feed_url))


def ensure_feed(redis_client, feed_url: str, last_ts: float | None) -> Dict[str, Any]:
    if last_ts is None:
        last_ts_value = time.time()
    else:
        try:
            last_ts_value = float(last_ts)
        except Exception:
            last_ts_value = time.time()

    return set_feed(
        redis_client,
        feed_url,
        {
            "last_ts": float(last_ts_value),
            "enabled": True,
            "portals": {},
        },
    )
