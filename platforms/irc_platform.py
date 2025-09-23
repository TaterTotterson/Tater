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
from helpers import LLMClientWrapper, parse_function_json, get_tater_name
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
LLM_MODEL = os.getenv("LLM_MODEL", "llama3")
llm_host = os.getenv("LLM_HOST", "127.0.0.1")
llm_port = os.getenv("LLM_PORT", "11434")
llm_client = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")

# ---- LM-Studio template helpers ----
def _to_template_msg(role, content, sender=None):
    """
    Shape messages for the Jinja template.
    - Strings -> keep as string (optionally prefix with sender)
    - plugin_response -> skip (return None)
    - plugin_call -> stringify JSON as assistant text
    """
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps({
            "function": content.get("plugin"),
            "arguments": content.get("arguments", {})
        }, indent=2)
        return {"role": "assistant" if role == "assistant" else role, "content": as_text}

    # IRC stores plain text; treat files already as placeholders elsewhere
    if isinstance(content, str):
        if role == "user" and sender:
            return {"role": "user", "content": f"{sender}: {content}"}
        return {"role": role, "content": content}

    # Fallback (shouldn’t really happen on IRC)
    return {"role": role, "content": str(content)}


def _enforce_user_assistant_alternation(loop_messages):
    """
    Merge consecutive same-role turns and ensure first turn is 'user'.
    Prevents the template's alternation error.
    """
    merged = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m)
            continue
        if merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            else:
                merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)

    if merged and merged[0]["role"] != "user":
        merged.insert(0, {"role": "user", "content": ""})
    return merged

def get_plugin_enabled(name):
    enabled = redis_client.hget("plugin_enabled", name)
    return enabled and enabled.lower() == "true"

def save_irc_message(channel, role, username, content):
    key = f"tater:irc:{channel}:history"
    max_store = int(redis_client.get("tater:max_store") or 20)
    redis_client.rpush(key, json.dumps({"role": role, "username": username, "content": content}))
    if max_store > 0:
        redis_client.ltrim(key, -max_store, -1)

def load_irc_history(channel, limit=None):
    if limit is None:
        limit = int(redis_client.get("tater:max_llm") or 8)
    key = f"tater:irc:{channel}:history"
    raw_history = redis_client.lrange(key, -limit, -1)

    loop_messages = []
    for entry in raw_history:
        data = json.loads(entry)
        role = data.get("role", "user")
        sender = data.get("username", role)
        content = data.get("content")

        # Represent non-text payloads as short placeholders (if you store them)
        if isinstance(content, dict) and content.get("type") in ["image", "audio", "video", "file"]:
            name = content.get("name", "file")
            content = f"[{content['type'].capitalize()}: {name}]"

        if role not in ("user", "assistant"):
            role = "assistant"

        templ = _to_template_msg(role, content, sender=sender if role == "user" else None)
        if templ is not None:
            loop_messages.append(templ)

    return _enforce_user_assistant_alternation(loop_messages)

def build_system_prompt():
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

    first, last = get_tater_name()
    base_prompt = (
        f"You are {first} {last}, an IRC-savvy AI assistant with access to various tools and plugins.\n\n"
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
        "Never call a tool in response to casual or friendly messages like 'thanks', 'lol', or 'cool' — reply normally instead.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}"
        "If no function is needed, reply normally.\n"
    )

@irc3.event(irc3.rfc.PRIVMSG)
async def on_message(self, mask, event, target, data):
    save_irc_message(channel=target, role="user", username=mask.nick, content=data)

    first, _ = get_tater_name()
    if first.lower() not in data.lower():
        return

    logger.info(f"<{mask.nick}> {data}")
    history = load_irc_history(channel=target)
    messages = [{"role": "system", "content": build_system_prompt()}] + history

    try:
        response = await llm_client.chat(messages)
        response_text = response["message"].get("content", "").strip()
        logger.info(f"{first}: {response_text}")

        parsed = parse_function_json(response_text)
        if parsed and isinstance(parsed, dict) and "function" in parsed:
            func = parsed["function"]
            args = parsed.get("arguments", {})

            if func in plugin_registry and get_plugin_enabled(func):
                plugin = plugin_registry[func]

                # Save structured plugin_call marker
                save_irc_message(channel=target, role="assistant", username="assistant", content={
                    "marker": "plugin_call",
                    "plugin": func,
                    "arguments": args
                })

                # Optional waiting message
                if hasattr(plugin, "waiting_prompt_template"):
                    wait_prompt = plugin.waiting_prompt_template.format(mention=mask.nick)
                    wait_response = await llm_client.chat(
                        messages=[
                            {"role": "system", "content": "Write one short, friendly status line."},
                            {"role": "user", "content": wait_prompt}
                        ]
                    )
                    wait_text = wait_response["message"]["content"].strip()
                    self.privmsg(target, wait_text)
                    save_irc_message(channel=target, role="assistant", username="assistant", content={
                        "marker": "plugin_response",
                        "content": wait_text
                    })

                result = await plugin.handle_irc(self, target, mask.nick, data, args, llm_client)

                if isinstance(result, list):
                    for item in result:
                        if isinstance(item, str):
                            for chunk in [item[i:i+MAX_RESPONSE_LENGTH] for i in range(0, len(item), MAX_RESPONSE_LENGTH)]:
                                self.privmsg(target, chunk)
                            save_irc_message(channel=target, role="assistant", username="assistant", content={
                                "marker": "plugin_response",
                                "content": item
                            })

                        elif isinstance(item, dict):
                            kind = item.get("type", "file").capitalize()
                            name = item.get("name", "output")
                            placeholder = f"[{kind}: {name}]"
                            self.privmsg(target, f"{mask.nick}: {placeholder}")
                            save_irc_message(channel=target, role="assistant", username="assistant", content={
                                "marker": "plugin_response",
                                "content": placeholder
                            })

                elif isinstance(result, str) and result.strip():
                    for chunk in [result[i:i+MAX_RESPONSE_LENGTH] for i in range(0, len(result), MAX_RESPONSE_LENGTH)]:
                        self.privmsg(target, chunk)
                    save_irc_message(channel=target, role="assistant", username="assistant", content={
                        "marker": "plugin_response",
                        "content": result
                    })

                else:
                    logger.debug(f"[{func}] Plugin returned nothing or unrecognized result type: {type(result)}")

                return

        # Default fallback reply
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