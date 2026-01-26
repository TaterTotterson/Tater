# plugin_settings.py
import os, redis, dotenv
dotenv.load_dotenv()

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)

def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return (enabled or "").lower() == "true"

def set_plugin_enabled(plugin_name: str, enabled: bool) -> None:
    redis_client.hset("plugin_enabled", plugin_name, "true" if enabled else "false")

def get_plugin_settings(category: str) -> dict:
    return redis_client.hgetall(f"plugin_settings:{category}")

def save_plugin_settings(category: str, settings: dict) -> None:
    redis_client.hset(f"plugin_settings:{category}", mapping={k: str(v) for k, v in settings.items()})