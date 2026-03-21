from pathlib import Path
from typing import Any, Callable


def normalize_abs_path(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return str(Path(raw).expanduser().resolve())
    except Exception:
        return raw


def redis_config_non_negative_int(
    key: str,
    default: int,
    *,
    redis_client: Any,
    coerce_non_negative_int_fn: Callable[[Any, int], int],
) -> int:
    try:
        raw = redis_client.get(key)
    except Exception:
        return max(0, int(default))
    return coerce_non_negative_int_fn(raw, default)


def redis_config_positive_int(
    key: str,
    default: int,
    *,
    redis_client: Any,
    redis_config_non_negative_int_fn: Callable[..., int],
) -> int:
    value = redis_config_non_negative_int_fn(key, default, redis_client=redis_client)
    if value <= 0:
        return max(1, int(default))
    return value


def configured_agent_state_ttl_seconds(
    *,
    redis_client: Any,
    key: str,
    default: int,
    redis_config_non_negative_int_fn: Callable[..., int],
) -> int:
    return redis_config_non_negative_int_fn(
        key,
        default,
        redis_client=redis_client,
    )


def configured_positive_int(
    *,
    redis_client: Any,
    key: str,
    default: int,
    redis_config_positive_int_fn: Callable[..., int],
) -> int:
    return redis_config_positive_int_fn(
        key,
        default,
        redis_client=redis_client,
    )
