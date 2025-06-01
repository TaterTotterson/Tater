# plugin_settings.py
import redis
import os
import dotenv

dotenv.load_dotenv()

redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

def get_plugin_enabled(plugin_name):
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return enabled and enabled.lower() == "true"

def get_plugin_settings(category):
    return redis_client.hgetall(f"plugin_settings:{category}")