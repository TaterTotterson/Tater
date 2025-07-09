# irc_platform.py
import os
import json
import redis
import logging
import asyncio
from datetime import datetime
from dotenv import load_dotenv
import re
import threading
from plugin_registry import plugin_registry
from helpers import OllamaClientWrapper, parse_function_json, send_waiting_message
import time
import sys
import irc3
from irc3.plugins.command import command

load_dotenv()
logger = logging.getLogger("irc.tater")

PLATFORM_SETTINGS = {
    "category": "IRC Settings",
    "required": {
        "irc_server": {
            "label": "IRC Server",
            "type": "string",
            "default": "irc.libera.chat",
            "description": "Hostname of the IRC server"
        },
        "irc_port": {
            "label": "IRC Port",
            "type": "string",
            "default": "6667",
            "description": "Port number"
        },
        "irc_channel": {
            "label": "IRC Channel",
            "type": "string",
            "default": "#tater",
            "description": "Channel to join"
        },
        "irc_nick": {
            "label": "IRC Nickname",
            "type": "string",
            "default": "TaterBot",
            "description": "Nickname to use"
        },
        "irc_username": {
            "label": "IRC Username",
            "type": "string",
            "default": "",
            "description": "Login username (for ZNC: typically username/network)"
        },
        "irc_password": {
            "label": "IRC Password",
            "type": "string",
            "default": "",
            "description": "Login password (ZNC password)"
        }

    }
}

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)

irc_settings = redis_client.hgetall("irc_platform_settings")
IRC_SERVER = irc_settings.get("irc_server", "irc.libera.chat")
IRC_PORT = int(irc_settings.get("irc_port", 6667))
IRC_CHANNEL = irc_settings.get("irc_channel", "#tater")
IRC_NICK = irc_settings.get("irc_nick", "TaterBot")
IRC_USERNAME = irc_settings.get("irc_username", "")
IRC_PASSWORD = irc_settings.get("irc_password", "")

MAX_RESPONSE_LENGTH = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
ollama_host = os.getenv("OLLAMA_HOST", "127.0.0.1")
ollama_port = os.getenv("OLLAMA_PORT", "11434")
ollama_client = OllamaClientWrapper(host=f"http://{ollama_host}:{ollama_port}")

def get_plugin_enabled(name):
    enabled = redis_client.hget("plugin_enabled", name)
    return enabled and enabled.lower() == "true"

def save_irc_message(channel, role, username, content):
    key = f"tater:irc:{channel}:history"
    max_store = int(redis_client.get("tater:max_store") or 20)
    redis_client.rpush(key, json.dumps({"role": role, "username": username, "content": content}))
    redis_client.ltrim(key, -max_store, -1)

def load_irc_history(channel, limit=None):
    if limit is None:
        limit = int(redis_client.get("tater:max_ollama") or 8)
    key = f"tater:irc:{channel}:history"
    raw_history = redis_client.lrange(key, -limit, -1)
    formatted = []
    for entry in raw_history:
        data = json.loads(entry)
        role = data.get("role", "user")
        sender = data.get("username", role)
        if role == "assistant":
            formatted.append({"role": "assistant", "content": data["content"]})
        else:
            formatted.append({"role": "user", "content": f"{sender}: {data['content']}"})
    return formatted

def build_system_prompt():
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

    base_prompt = (
        "You are Tater Totterson, an IRC-savvy AI assistant with access to various tools and plugins.\n\n"
        "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
    )

    tool_instructions = "\n\n".join(
        f"Tool: {plugin.name}\n"
        f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
        f"{plugin.usage}"
        for plugin in plugin_registry.values()
        if ("irc" in plugin.platforms or "both" in plugin.platforms) and get_plugin_enabled(plugin.name)
    )

    behavior_guard = (
        "Only call a tool if the user's latest message clearly requests an action — such as 'generate', 'summarize', or 'download'.\n"
        "Do not call a tool in response to casual or friendly messages like 'thanks', 'lol', or 'cool' — reply normally instead.\n"
        "Never mimic earlier responses or patterns — always respond based on the user's current intent only.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}"
        "If no function is needed, reply normally."
    )

@irc3.event(irc3.rfc.PRIVMSG)
async def on_message(self, mask, event, target, data):
    save_irc_message(channel=target, role="user", username=mask.nick, content=data)

    if "tater" not in data.lower():
        return

    logger.info(f"<{mask.nick}> {data}")
    history = load_irc_history(channel=target)
    messages = [{"role": "system", "content": build_system_prompt()}] + history

    try:
        response = await ollama_client.chat(messages)
        response_text = response["message"].get("content", "").strip()
        logger.info(f"Tater: {response_text}")

        parsed = parse_function_json(response_text)
        if parsed and isinstance(parsed, dict) and "function" in parsed:
            func = parsed["function"]
            args = parsed.get("arguments", {})

            if func in plugin_registry and get_plugin_enabled(func):
                plugin = plugin_registry[func]

                # Waiting message
                if hasattr(plugin, "waiting_prompt_template"):
                    wait_prompt = plugin.waiting_prompt_template.format(mention=mask.nick)
                    wait_response = await ollama_client.chat(
                        messages=[{"role": "user", "content": wait_prompt}]
                    )
                    wait_text = wait_response["message"]["content"].strip()
                    self.privmsg(target, wait_text)
                    save_irc_message(channel=target, role="assistant", username="assistant", content=wait_text)

                result = await plugin.handle_irc(
                    self, target, mask.nick, data, args, ollama_client
                )

                if isinstance(result, list):
                    for item in result:
                        if isinstance(item, str):
                            for chunk in [item[i:i+MAX_RESPONSE_LENGTH] for i in range(0, len(item), MAX_RESPONSE_LENGTH)]:
                                self.privmsg(target, chunk)
                            save_irc_message(channel=target, role="assistant", username="assistant", content=item)

                        elif isinstance(item, dict):
                            kind = item.get("type")
                            name = item.get("name", "output")
                            placeholder = f"[{kind.capitalize()}: {name}]"
                            self.privmsg(target, f"{mask.nick}: {placeholder}")
                            save_irc_message(channel=target, role="assistant", username="assistant", content=placeholder)

                elif isinstance(result, str) and result.strip():
                    for chunk in [result[i:i+MAX_RESPONSE_LENGTH] for i in range(0, len(result), MAX_RESPONSE_LENGTH)]:
                        self.privmsg(target, chunk)
                    save_irc_message(channel=target, role="assistant", username="assistant", content=result)

                else:
                    logger.debug(f"[{func}] Plugin returned nothing or unrecognized result type: {type(result)}")

                return

        # Default fallback (not tool-based)
        for chunk in [response_text[i:i+MAX_RESPONSE_LENGTH] for i in range(0, len(response_text), MAX_RESPONSE_LENGTH)]:
            self.privmsg(target, chunk)
        save_irc_message(channel=target, role="assistant", username="assistant", content=response_text)

    except Exception as e:
        logger.error(f"Error processing IRC message: {e}")
        self.privmsg(target, f"{mask.nick}: Sorry, I ran into an error while thinking.")

def run(stop_event=None):
    # 1) Build your IRC config
    config = {
        "nick": IRC_NICK,
        "autojoins": [IRC_CHANNEL],
        "host": IRC_SERVER,
        "port": IRC_PORT,
        "ssl": False,
        "includes": [__name__],
    }
    if IRC_USERNAME and IRC_PASSWORD:
        config["username"] = IRC_USERNAME
        config["password"] = IRC_PASSWORD

    # 2) Spin up a fresh event loop for this platform
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    bot = irc3.IrcBot(loop=loop, **config)

    # 3) Single coroutine to run the bot and watch for stop_event
    async def bot_runner():
        try:
            bot.create_connection()
            logger.info("✅ IRC bot connected.")

            # If no stop_event was passed, just run forever
            if not stop_event:
                await asyncio.Event().wait()

            # Otherwise, poll the threading.Event
            while not stop_event.is_set():
                await asyncio.sleep(1)

            logger.info("🛑 stop_event triggered, shutting down IRC bot...")
            bot.quit("Shutting down.")
            await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            # expected on shutdown
            pass
        except Exception as e:
            logger.error(f"❌ IRC bot error: {e}")

    # 4) Run it & ensure clean loop shutdown
    try:
        loop.run_until_complete(bot_runner())
    finally:
        # cancel any leftover tasks
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()