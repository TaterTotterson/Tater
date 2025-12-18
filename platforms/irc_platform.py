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
import time
import sys
import irc3
from irc3.plugins.command import command
import textwrap
from helpers import (
    parse_function_json,
    get_tater_name,
    get_tater_personality,
    get_llm_client_from_env,
    build_llm_host_from_env,
)

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

MAX_RESPONSE_LENGTH = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))
llm_client = None

# ---- Fresh settings + formatting helpers ----
IRC_PRIVMSG_LIMIT = int(os.getenv("IRC_PRIVMSG_LIMIT", 430))  # safe default for IRC line length

def _load_irc_settings():
    """Fetch current IRC settings from Redis, with fallback defaults."""
    settings = (
        redis_client.hgetall("irc_platform_settings")
        or redis_client.hgetall("platform_settings:IRC Settings")
        or redis_client.hgetall("platform_settings:IRC")
        or {}
    )

    server = settings.get("irc_server", "irc.libera.chat")
    port = int(settings.get("irc_port", 6667)) if str(settings.get("irc_port", "")).strip() else 6667
    channel = settings.get("irc_channel", "#tater")
    if channel and not channel.startswith("#"):
        channel = f"#{channel}"
    nick = settings.get("irc_nick", "TaterBot")
    username = settings.get("irc_username", "")
    password = settings.get("irc_password", "")
    ssl_flag = str(settings.get("irc_ssl", "false")).lower() in ("1", "true", "yes", "on")

    return {
        "server": server,
        "port": port,
        "channel": channel,
        "nick": nick,
        "username": username,
        "password": password,
        "ssl": ssl_flag,
    }

def format_irc_text(raw: str, width: int = 80) -> str:
    if not isinstance(raw, str):
        return str(raw)

    text = raw.strip()
    text = re.sub(r'(?m)^\s*(\d+)\.\s*\n+\s*', r'\1. ', text)   # "N.\nTitle" -> "N. Title"
    text = re.sub(r'(?m)\s*\n\s*-\s+', ' - ', text)             # "Title\n- Subtitle" -> "Title - Subtitle"
    text = re.sub(r'(?m)\s*\n\((\d{4})\)', r' (\1)', text)      # "Title\n(2025)" -> "Title (2025)"

    lines = text.splitlines()
    out, current = [], []
    in_code = False

    def flush():
        nonlocal current
        if current:
            out.append(" ".join(s.strip() for s in current if s.strip()))
            current = []

    for line in lines:
        s = line.rstrip()
        if s.strip().startswith("```"):
            flush()
            in_code = not in_code
            out.append(s)
            continue
        if in_code:
            out.append(s)
            continue
        if re.match(r'^\s*\d+\.\s+', s):
            flush()
            current.append(s.strip())
            continue
        if not s.strip():
            flush()
            out.append("")
            continue
        current.append(s)

    flush()

    result = "\n".join(out)
    result = re.sub(r'\n{3,}', '\n\n', result)
    result = re.sub(r'(?s)^(.*?\S)\s+(?=\d+\.\s)', r'\1\n\n', result, count=1)  # header sep before first item
    return result

def send_formatted(self, target, text):
    formatted = format_irc_text(text)

    # Ensure each numbered item starts a new paragraph even if inline
    normalized = re.sub(r'(?<!\n)\s+(\d+\.\s+)', r'\n\n\1', formatted)

    # Split into paragraphs on blank lines
    paragraphs = re.split(r'\n\s*\n+', normalized.strip())
    lim = IRC_PRIVMSG_LIMIT

    def send_chunked(s: str):
        s = s.strip()
        while len(s) > lim:
            cut = s.rfind(" ", 0, lim)
            if cut == -1:
                for ch in ("/", "-", "_", "|", ",", ";", ":"):
                    cut = s.rfind(ch, 0, lim)
                    if cut != -1:
                        cut += 1
                        break
            if cut == -1:
                cut = lim
            self.privmsg(target, s[:cut].rstrip())
            time.sleep(0.05)
            s = s[cut:].lstrip()
        if s:
            self.privmsg(target, s)
            time.sleep(0.02)

    for idx, para in enumerate(paragraphs):
        p = para.strip()
        if p.startswith("```") and p.endswith("```"):
            for line in p.splitlines():
                send_chunked(line)
        else:
            # collapse internal newlines inside a paragraph to single spaces
            one_line = re.sub(r'\s*\n\s*', ' ', p)
            send_chunked(one_line)

        if idx < len(paragraphs) - 1:
            self.privmsg(target, " ")

# ---- LM template helpers ----
def _to_template_msg(role, content, sender=None):
    # --- Skip waiting lines from tools ---
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # --- Include only FINAL plugin responses ---
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        phase = content.get("phase", "final")
        if phase != "final":
            return None
        payload = content.get("content")

        # 1) Plain string
        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " …"
            return {"role": "assistant", "content": txt}

        # 2) Media placeholders
        if isinstance(payload, dict) and payload.get("type") in ("image", "audio", "video", "file"):
            kind = payload.get("type").capitalize()
            name = payload.get("name") or ""
            return {"role": "assistant", "content": f"[{kind} from tool]{f' {name}' if name else ''}".strip()}

        # 3) Fallback: compact JSON
        try:
            compact = json.dumps(payload, ensure_ascii=False)
            if len(compact) > 2000:
                compact = compact[:2000] + " …"
            return {"role": "assistant", "content": compact}
        except Exception:
            return None

    # --- Represent plugin calls as plain text (so history still makes sense) ---
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps({
            "function": content.get("plugin"),
            "arguments": content.get("arguments", {})
        }, indent=2)
        return {"role": "assistant" if role == "assistant" else role, "content": as_text}

    # --- Text path ---
    if isinstance(content, str):
        if role == "user" and sender:
            return {"role": "user", "content": f"{sender}: {content}"}
        return {"role": role, "content": content}

    # Fallback
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
    personality = get_tater_personality()

    persona_clause = ""
    if personality:
        persona_clause = (
            f"You should speak and behave like {personality} "
            "while still being helpful, concise, and easy to understand. "
            "Keep the style subtle rather than over-the-top. "
            "Even while staying in character, you must strictly follow the tool-calling rules below.\n\n"
        )

    base_prompt = (
        f"You are {first} {last}, an IRC-savvy AI assistant with access to various tools and plugins.\n\n"
        f"{persona_clause}"
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

                # Save structured plugin_call marker (corrected)
                save_irc_message(
                    channel=target,
                    role="assistant",
                    username="assistant",
                    content={"marker": "plugin_call", "plugin": func, "arguments": args}
                )

                # Optional waiting message (save as plugin_wait AFTER generated)
                if hasattr(plugin, "waiting_prompt_template"):
                    wait_prompt = plugin.waiting_prompt_template.format(mention=mask.nick)
                    wait_response = await llm_client.chat(
                        messages=[
                            {"role": "system", "content": "Write one short, friendly status line."},
                            {"role": "user", "content": wait_prompt}
                        ]
                    )
                    wait_text = wait_response["message"]["content"].strip()
                    send_formatted(self, target, wait_text)
                    save_irc_message(
                        channel=target,
                        role="assistant",
                        username="assistant",
                        content={"marker": "plugin_wait", "content": wait_text}
                    )

                result = await plugin.handle_irc(self, target, mask.nick, data, args, llm_client)

                if isinstance(result, list):
                    for item in result:
                        if isinstance(item, str):
                            send_formatted(self, target, item)
                            save_irc_message(
                                channel=target,
                                role="assistant",
                                username="assistant",
                                content={"marker": "plugin_response", "phase": "final", "content": item}
                            )

                        elif isinstance(item, dict):
                            # Save a light placeholder to history
                            kind = (item.get("type") or "file").lower()
                            name = item.get("name", "output")
                            placeholder = f"[{kind.capitalize()}: {name}]"
                            self.privmsg(target, f"{mask.nick}: {placeholder}")
                            save_irc_message(
                                channel=target,
                                role="assistant",
                                username="assistant",
                                content={
                                    "marker": "plugin_response",
                                    "phase": "final",
                                    "content": {"type": kind, "name": name}
                                }
                            )

                elif isinstance(result, str) and result.strip():
                    send_formatted(self, target, result)
                    save_irc_message(
                        channel=target,
                        role="assistant",
                        username="assistant",
                        content={"marker": "plugin_response", "phase": "final", "content": result}
                    )

                else:
                    logger.debug(f"[{func}] Plugin returned nothing or unrecognized result type: {type(result)}")

                return

        # Default fallback reply
        send_formatted(self, target, response_text)
        save_irc_message(channel=target, role="assistant", username="assistant", content=response_text)

    except Exception as e:
        logger.error(f"Error processing IRC message: {e}")
        self.privmsg(target, f"{mask.nick}: Sorry, I ran into an error while thinking.")

def run(stop_event=None):

    global llm_client
    llm_client = get_llm_client_from_env()

    # Load fresh settings each time the platform starts
    cfg = _load_irc_settings()

    # Build your IRC config from current Redis values
    config = {
        "nick": cfg["nick"],
        "autojoins": [cfg["channel"]],
        "host": cfg["server"],
        "port": cfg["port"],
        "ssl": cfg["ssl"],
        "includes": [__name__],
    }
    if cfg["username"] and cfg["password"]:
        config["username"] = cfg["username"]
        config["password"] = cfg["password"]

    logger.info(
        f"IRC connecting to {cfg['server']}:{cfg['port']} "
        f"as {cfg['nick']} in {cfg['channel']} (SSL={cfg['ssl']})"
    )

    # Spin up a fresh event loop for this platform
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    bot = irc3.IrcBot(loop=loop, **config)

    # Single coroutine to run the bot and watch for stop_event
    async def bot_runner():
        try:
            bot.create_connection()
            logger.info("✅ IRC bot connected.")

            # If no stop_event was passed, run forever
            if not stop_event:
                await asyncio.Event().wait()

            # Otherwise, poll the threading.Event
            while not stop_event.is_set():
                await asyncio.sleep(1)

            logger.info("🛑 stop_event triggered, shutting down IRC bot...")
            bot.quit("Shutting down.")
            await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            pass  # expected on shutdown
        except Exception as e:
            logger.error(f"❌ IRC bot error: {e}")

    # Run it & ensure clean loop shutdown
    try:
        loop.run_until_complete(bot_runner())
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()