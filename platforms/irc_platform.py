# irc_platform.py
import os
import json
import redis
import logging
import asyncio
from dotenv import load_dotenv
import re
import plugin_registry as pr
import time
import irc3
from notify.queue import is_expired
from helpers import (
    get_tater_name,
    get_llm_client_from_env,
)
from admin_gate import (
    is_admin_only_plugin,
    normalize_admin_list,
)
from plugin_result import action_failure
from plugin_kernel import plugin_supports_platform
from cerberus import run_cerberus_turn, resolve_agent_limits

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
        },
        "admin_nick": {
            "label": "Admin Nick",
            "type": "string",
            "default": "",
            "description": "Only this nick can run admin-only tools (comma-separated supported)."
        }
    }
}

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)
NOTIFY_QUEUE_KEY = "notifyq:irc"
NOTIFY_POLL_INTERVAL = 0.5

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
    admin_nick = settings.get("admin_nick", "")

    return {
        "server": server,
        "port": port,
        "channel": channel,
        "nick": nick,
        "username": username,
        "password": password,
        "ssl": ssl_flag,
        "admin_nick": admin_nick,
    }

def _irc_admin_allowed(nick: str) -> bool:
    raw = (_load_irc_settings().get("admin_nick") or "").strip()
    allowed = normalize_admin_list(raw)
    if not allowed:
        return False
    return str(nick or "").strip().lower() in allowed

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

async def _notify_queue_worker(bot, stop_event=None):
    while True:
        if stop_event and stop_event.is_set():
            break

        try:
            item_json = await asyncio.to_thread(redis_client.lpop, NOTIFY_QUEUE_KEY)
        except Exception:
            item_json = None

        if not item_json:
            await asyncio.sleep(NOTIFY_POLL_INTERVAL)
            continue

        try:
            item = json.loads(item_json)
        except Exception:
            logger.warning("[notifyq] invalid JSON item; skipping.")
            continue

        if is_expired(item):
            continue

        targets = item.get("targets") or {}
        channel = targets.get("channel")
        if not channel:
            logger.warning("[notifyq] IRC missing channel; dropping item.")
            continue

        message = (item.get("message") or "").strip()
        if not message:
            continue

        title = item.get("title")
        payload = f"{title}\n{message}" if title else message
        send_formatted(bot, channel, payload)

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
    Merge consecutive same-role turns to keep history compact.

    IMPORTANT:
    Do NOT insert a blank user message at the beginning.
    Some LLM backends (LM Studio, Qwen, etc.) can return empty
    completions when an empty user turn (content="") appears.
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

    return merged


def _strip_bot_mention(text: str, bot_name: str) -> str:
    if not text:
        return ""
    if not bot_name:
        return text.strip()
    pattern = re.compile(rf"\\b{re.escape(bot_name)}\\b", flags=re.IGNORECASE)
    cleaned = pattern.sub("", text)
    cleaned = re.sub(r"[@,:;.!]+", " ", cleaned)
    cleaned = re.sub(r"\\s+", " ", cleaned).strip()
    return cleaned


def _is_mention_only(text: str, bot_name: str) -> bool:
    return _strip_bot_mention(text, bot_name) == ""


def _sanitize_request_text(text: str, bot_name: str = "", sender_name: str = "") -> str:
    if not text:
        return ""
    cleaned = str(text).strip()
    if sender_name:
        prefix = re.compile(rf"^@?{re.escape(sender_name)}\\b\\s*[:,-]\\s*", flags=re.IGNORECASE)
        cleaned = prefix.sub("", cleaned).strip()
    if bot_name:
        cleaned = _strip_bot_mention(cleaned, bot_name)
    return cleaned


def _find_prev_user_message(channel: str, sender: str, bot_name: str, exclude_text=None) -> str:
    key = f"tater:irc:{channel}:history"
    try:
        raw_history = redis_client.lrange(key, 0, -1)
    except Exception:
        return ""

    for entry in reversed(raw_history):
        try:
            data = json.loads(entry)
        except Exception:
            continue
        if data.get("role") != "user":
            continue
        if sender and data.get("username") != sender:
            continue
        content = data.get("content")
        if not isinstance(content, str):
            continue
        if exclude_text and content == exclude_text:
            continue
        if _is_mention_only(content, bot_name):
            continue
        return content.strip()

    return ""

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
    # Platform preamble should be style/format only.
    return (
        "You are an IRC-savvy AI assistant.\n"
        "Use plain ASCII text only; no markdown and no emoji.\n"
        "Keep replies concise and natural for chat.\n"
    )

@irc3.event(irc3.rfc.PRIVMSG)
async def on_message(self, mask, event, target, data):
    save_irc_message(channel=target, role="user", username=mask.nick, content=data)

    first, _ = get_tater_name()
    if first.lower() not in data.lower():
        return

    effective_request = data
    stripped = _strip_bot_mention(data, first)
    if stripped:
        effective_request = stripped
    elif _is_mention_only(data, first):
        prev = _find_prev_user_message(target, mask.nick, first, exclude_text=data)
        if prev:
            effective_request = prev
    effective_request = _sanitize_request_text(effective_request, bot_name=first, sender_name=mask.nick)

    logger.info(f"<{mask.nick}> {data}")
    history = load_irc_history(channel=target)
    messages = history
    platform_preamble = build_system_prompt()
    merged_registry = dict(pr.get_registry_snapshot() or {})
    merged_enabled = get_plugin_enabled

    try:
        scope_value = f"chan:{target}" if str(target or "").startswith(("#", "&")) else f"pm:{target}"
        origin = {
            "platform": "irc",
            "channel": target,
            "target": target,
            "chat_type": "channel" if str(target or "").startswith(("#", "&")) else "pm",
            "user": mask.nick,
            "request_id": f"{target}:{time.time():.3f}",
        }
        origin = {k: v for k, v in origin.items() if v not in (None, "")}

        async def _wait_callback(func_name, plugin_obj):
            if not plugin_obj:
                return
            if not plugin_supports_platform(plugin_obj, "irc"):
                return
            if not hasattr(plugin_obj, "waiting_prompt_template"):
                return
            wait_msg = plugin_obj.waiting_prompt_template.format(mention=mask.nick)
            wait_response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "Write one short, plain ASCII status line."},
                    {"role": "user", "content": wait_msg},
                ]
            )
            wait_text = str((wait_response.get("message", {}) or {}).get("content", "") or "").strip()
            if wait_text:
                self.privmsg(target, f"{mask.nick}: {wait_text}")
                save_irc_message(
                    channel=target,
                    role="assistant",
                    username="assistant",
                    content={"marker": "plugin_wait", "content": wait_text},
                )

        def _admin_guard(func_name):
            needs_admin = False
            if is_admin_only_plugin(func_name):
                needs_admin = True

            if needs_admin and not _irc_admin_allowed(mask.nick):
                msg = (
                    "This tool is restricted to the configured admin user on IRC."
                    if (_load_irc_settings().get("admin_nick") or "").strip()
                    else "This tool is disabled because no IRC admin user is configured."
                )
                return action_failure(
                    code="admin_only",
                    message=msg,
                    needs=[],
                    say_hint="Explain that this tool is restricted to the admin user on this platform.",
                )
            return None

        agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
        result = await run_cerberus_turn(
            llm_client=llm_client,
            platform="irc",
            history_messages=messages,
            registry=merged_registry,
            enabled_predicate=merged_enabled,
            context={
                "bot": self,
                "channel": target,
                "user": mask.nick,
                "raw_message": data,
                "raw": data,
            },
            user_text=effective_request,
            scope=scope_value,
            origin=origin,
            wait_callback=_wait_callback,
            admin_guard=_admin_guard,
            redis_client=redis_client,
            max_rounds=agent_max_rounds,
            max_tool_calls=agent_max_tool_calls,
            platform_preamble=platform_preamble,
        )

        final_text = str(result.get("text") or "").strip()
        if final_text:
            send_formatted(self, target, final_text)
            save_irc_message(
                channel=target,
                role="assistant",
                username="assistant",
                content={"marker": "plugin_response", "phase": "final", "content": final_text},
            )

        artifacts = result.get("artifacts") or []
        for item in artifacts:
            if not isinstance(item, dict):
                continue
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
                    "content": {"type": kind, "name": name},
                },
            )
        return

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

            worker_task = asyncio.create_task(_notify_queue_worker(bot, stop_event))

            # If no stop_event was passed, run forever
            if not stop_event:
                await asyncio.Event().wait()

            # Otherwise, poll the threading.Event
            while not stop_event.is_set():
                await asyncio.sleep(1)

            logger.info("🛑 stop_event triggered, shutting down IRC bot...")
            if worker_task and not worker_task.done():
                worker_task.cancel()
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
