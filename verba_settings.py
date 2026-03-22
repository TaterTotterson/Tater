import dotenv
from helpers import redis_client

dotenv.load_dotenv()

VERBA_ENABLED_HASH = "verba_enabled"
VERBA_SETTINGS_PREFIX = "verba_settings:"


def _to_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def get_verba_enabled(verba_name: str) -> bool:
    value = redis_client.hget(VERBA_ENABLED_HASH, verba_name)
    return _to_bool(str(value or ""))


def set_verba_enabled(verba_name: str, enabled: bool) -> None:
    value = "true" if enabled else "false"
    redis_client.hset(VERBA_ENABLED_HASH, verba_name, value)


def get_verba_settings(category: str) -> dict:
    return redis_client.hgetall(f"{VERBA_SETTINGS_PREFIX}{category}") or {}


def save_verba_settings(category: str, settings: dict) -> None:
    mapping = {k: str(v) for k, v in (settings or {}).items()}
    redis_client.hset(f"{VERBA_SETTINGS_PREFIX}{category}", mapping=mapping)
