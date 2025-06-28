# platform/irc_platform.py
import os
import json
import redis
import logging
import asyncio
import irc3
from irc3.plugins.command import command
from datetime import datetime
from dotenv import load_dotenv
from plugin_registry import plugin_registry
from helpers import OllamaClientWrapper, parse_function_json

load_dotenv()
logger = logging.getLogger("irc.tater")
logger.setLevel(logging.INFO)  # ensure our INFO logs appear

# IRC settings schema (pulled from Redis at startup)
PLATFORM_SETTINGS = {
    "category": "IRC Settings",
    "required": {
        "irc_server": {"label": "IRC Server", "type": "string", "default": "irc.libera.chat", "description": "Hostname of the IRC server"},
        "irc_port":   {"label": "IRC Port",   "type": "string", "default": "6667",           "description": "Port number"},
        "irc_channel":{"label": "IRC Channel","type": "string", "default": "#tater",         "description": "Channel to join"},
        "irc_nick":   {"label": "IRC Nickname","type": "string", "default": "TaterBot",      "description": "Nickname to use"},
        "irc_username":{"label":"IRC Username","type":"string", "default":"",               "description":"Login username"},
        "irc_password":{"label":"IRC Password","type":"string", "default":"",               "description":"Login password"},
    }
}

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)

irc_settings = redis_client.hgetall("irc_platform_settings")
IRC_SERVER   = irc_settings.get("irc_server",  "irc.libera.chat")
IRC_PORT     = int(irc_settings.get("irc_port", 6667))
IRC_CHANNEL  = irc_settings.get("irc_channel", "#tater")
IRC_NICK     = irc_settings.get("irc_nick",    "TaterBot")
IRC_USERNAME = irc_settings.get("irc_username","")
IRC_PASSWORD = irc_settings.get("irc_password","")

MAX_RESPONSE_LENGTH = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))
OLLAMA_MODEL       = os.getenv("OLLAMA_MODEL", "llama3")
ollama_host        = os.getenv("OLLAMA_HOST",  "127.0.0.1")
ollama_port        = int(os.getenv("OLLAMA_PORT", 11434))
ollama_client      = OllamaClientWrapper(host=f"http://{ollama_host}:{ollama_port}")

def get_plugin_enabled(name: str) -> bool:
    val = redis_client.hget("plugin_enabled", name)
    return bool(val and val.lower() == "true")

def save_irc_message(channel: str, role: str, username: str, content: str):
    key = f"tater:irc:{channel}:history"
    redis_client.rpush(key, json.dumps({"role": role, "username": username, "content": content}))
    redis_client.ltrim(key, -20, -1)

def load_irc_history(channel: str, limit: int = 20):
    key   = f"tater:irc:{channel}:history"
    raw   = redis_client.lrange(key, -limit, -1)
    out   = []
    for entry in raw:
        data = json.loads(entry)
        role = data.get("role", "user")
        sender = data.get("username", role)
        if role == "assistant":
            out.append({"role": "assistant", "content": data["content"]})
        else:
            out.append({"role": "user", "content": f"{sender}: {data['content']}"})
    return out

BASE_PROMPT = (
    "You are Tater Totterson, an IRC-savvy AI assistant with access to various tools and plugins.\n\n"
    "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
)

def build_system_prompt() -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    tool_instructions = "\n\n".join(
        f"Tool: {plugin.name}\n"
        f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
        f"{plugin.usage}"
        for plugin in plugin_registry.values()
        if ("irc" in plugin.platforms or "both" in plugin.platforms) and get_plugin_enabled(plugin.name)
    )
    return (
        f"Current Date and Time is: {now}\n\n"
        f"{BASE_PROMPT}\n\n"
        f"{tool_instructions}\n\n"
        "Only consider the most recent user message when deciding whether to call a tool.\n"
        "Ignore earlier messages in the conversation for tool use decisions.\n"
        "If no function is clearly required based on the last message alone, respond normally.\n"
    )

@irc3.plugin
class irc_platform:
    def __init__(self, bot):
        self.bot = bot

    @irc3.event(irc3.rfc.PRIVMSG)
    def on_message(self, mask, event, target, data):
        # Fire-and-forget into our async handler
        asyncio.create_task(self.handle_message(mask, target, data))

    async def handle_message(self, mask, target, data):
        save_irc_message(channel=target, role="user", username=mask.nick, content=data)

        if "tater" not in data.lower():
            return

        logger.info(f"<{mask.nick}> {data}")
        history  = load_irc_history(channel=target, limit=20)
        messages = [{"role": "system", "content": build_system_prompt()}] + history

        try:
            resp = await ollama_client.chat(
                model=OLLAMA_MODEL,
                messages=messages,
                stream=False,
                keep_alive=-1,
                options={"num_ctx": ollama_client.context_length}
            )
            response_text = resp.get("message", {}).get("content", "").strip()
            logger.info(f"Tater raw response: {response_text!r}")

            parsed = parse_function_json(response_text)
            if parsed and isinstance(parsed, dict) and "function" in parsed:
                func = parsed["function"]
                args = parsed.get("arguments", {})
                if func in plugin_registry and get_plugin_enabled(func):
                    # call the plugin
                    plugin = plugin_registry[func]
                    result = await plugin.handle_irc(
                        self.bot, target, mask.nick, data, args, ollama_client
                    )
                    if result and result.strip():
                        self.bot.privmsg(target, result)
                        save_irc_message(channel=target, role="assistant", username="assistant", content=result)
                    else:
                        logger.warning(f"Plugin '{func}' returned empty. Sending raw JSON fallback.")
                        self.bot.privmsg(target, response_text)
                        save_irc_message(channel=target, role="assistant", username="assistant", content=response_text)
                else:
                    logger.info(f"Function '{func}' not enabled or not found. Sending raw JSON fallback.")
                    self.bot.privmsg(target, response_text)
                    save_irc_message(channel=target, role="assistant", username="assistant", content=response_text)
            else:
                # Plain-text response path
                for i in range(0, len(response_text), MAX_RESPONSE_LENGTH):
                    self.bot.privmsg(target, response_text[i:i+MAX_RESPONSE_LENGTH])
                save_irc_message(channel=target, role="assistant", username="assistant", content=response_text)

        except Exception as e:
            logger.error(f"Error processing IRC message: {e}", exc_info=True)
            self.bot.privmsg(target, f"{mask.nick}: Sorry, I ran into an error while thinking.")

def run():
    config = {
        "nick":     IRC_NICK,
        "autojoins":[IRC_CHANNEL],
        "host":     IRC_SERVER,
        "port":     IRC_PORT,
        "ssl":      False,
        "includes": [__name__],
        "loop":     asyncio.get_event_loop(),
    }
    if IRC_USERNAME and IRC_PASSWORD:
        config["username"] = IRC_USERNAME
        config["password"] = IRC_PASSWORD

    bot = irc3.IrcBot(**config)
    bot.run(forever=True)