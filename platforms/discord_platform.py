# discord_platform.py
import os
import json
import asyncio
import logging
import redis
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
import re
from datetime import datetime
from plugin_registry import plugin_registry
from helpers import LLMClientWrapper, parse_function_json, get_tater_name, get_tater_personality
import logging
import threading
import signal
import time
import base64
from io import BytesIO


load_dotenv()
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
max_response_length = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('discord')

redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

PLATFORM_SETTINGS = {
    "category": "Discord Settings",
    "required": {
        "discord_token": {
            "label": "Discord Bot Token",
            "type": "string",
            "default": "",
            "description": "Your Discord bot token"
        },
        "admin_user_id": {
            "label": "Admin User ID",
            "type": "string",
            "default": "",
            "description": "User ID allowed to DM the bot"
        },
        "response_channel_id": {
            "label": "Response Channel ID",
            "type": "string",
            "default": "",
            "description": "Channel where the assistant replies"
        }
    }
}

# ---- LM template helpers ----
def _to_template_msg(role, content, sender=None):
    """
    Shape messages for the Jinja template.
    - Strings -> keep as string (optionally prefix with sender for multi-user rooms)
    - Images  -> [{"type":"image"}] (placeholder)
    - Audio   -> [{"type":"text","text":"[Audio]"}] (placeholder)
    - plugin_wait -> skip
    - plugin_response (final) -> include text / placeholders / compact JSON
    - plugin_call -> stringify JSON as assistant text
    """

    # --- Skip waiting lines from tools ---
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # --- Include final plugin responses in context (text only / placeholders) ---
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
        if isinstance(payload, dict) and payload.get("type") in ("image", "audio", "video"):
            kind = payload.get("type")
            name = payload.get("name") or ""
            return {"role": "assistant", "content": f"[{kind.capitalize()} from tool]{f' {name}' if name else ''}".strip()}

        # 3) Structured text fields
        if isinstance(payload, dict):
            for key in ("summary", "text", "message", "content"):
                if isinstance(payload.get(key), str) and payload.get(key).strip():
                    txt = payload[key].strip()
                    if len(txt) > 4000:
                        txt = txt[:4000] + " …"
                    return {"role": "assistant", "content": txt}
            # Fallback: compact JSON
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

    # --- Media placeholders ---
    if isinstance(content, dict) and content.get("type") == "image":
        return {"role": role, "content": [{"type": "image"}]}
    if isinstance(content, dict) and content.get("type") == "audio":
        return {"role": role, "content": [{"type": "text", "text": "[Audio]"}]}
    if isinstance(content, dict) and content.get("type") == "video":
        return {"role": role, "content": [{"type": "text", "text": "[Video]"}]}

    # --- Text + fallback ---
    if isinstance(content, str):
        if role == "user" and sender:
            return {"role": "user", "content": f"{sender}: {content}"}
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}


def _enforce_user_assistant_alternation(loop_messages):
    """
    Merge consecutive same-role turns and ensure the first non-system is 'user'.
    This avoids the template's alternation exception.
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
            elif isinstance(a, list) and isinstance(b, list):
                merged[-1]["content"] = a + b
            else:
                merged[-1]["content"] = ( (a if isinstance(a, str) else str(a)) +
                                          "\n\n" +
                                          (b if isinstance(b, str) else str(b)) ).strip()
        else:
            merged.append(m)

    if merged and merged[0]["role"] != "user":
        merged.insert(0, {"role": "user", "content": ""})
    return merged

def get_plugin_enabled(plugin_name):
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return enabled and enabled.lower() == "true"

def clear_channel_history(channel_id):
    key = f"tater:channel:{channel_id}:history"
    try:
        redis_client.delete(key)
        logger.info(f"Cleared chat history for channel {channel_id}.")
    except Exception as e:
        logger.error(f"Error clearing chat history for channel {channel_id}: {e}")
        raise

async def safe_send(channel, content, max_length=2000):
    for i in range(0, len(content), max_length):
        await channel.send(content[i:i+max_length])

class discord_platform(commands.Bot):
    def __init__(self, llm_client, admin_user_id, response_channel_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.llm = llm_client
        self.admin_user_id = admin_user_id
        self.response_channel_id = response_channel_id
        self.max_response_length = max_response_length

    def build_system_prompt(self):
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
            f"You are {first} {last}, a Discord-savvy AI assistant with access to various tools and plugins.\n\n"
            f"{persona_clause}"
            "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
        )

        tool_instructions = "\n\n".join(
            f"Tool: {plugin.name}\n"
            f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
            f"{plugin.usage}"
            for plugin in plugin_registry.values()
            if ("discord" in plugin.platforms or "both" in plugin.platforms) and get_plugin_enabled(plugin.name)
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
            "If no function is needed, reply normally."
        )

    async def setup_hook(self):
        await self.add_cog(AdminCommands(self))
        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} app commands.")
        except Exception as e:
            logger.error(f"Failed to sync app commands: {e}")

    async def on_ready(self):
        first, last = get_tater_name()
        activity = discord.Activity(name=first.lower(), state=last, type=discord.ActivityType.custom)
        await self.change_presence(activity=activity)
        logger.info(f"Bot is ready. Admin: {self.admin_user_id}, Response Channel: {self.response_channel_id}")

    async def generate_error_message(self, prompt: str, fallback: str, message: discord.Message):
        try:
            error_response = await self.llm.chat(
                messages=[
                    {"role": "system", "content": "Write a short, friendly, plain-English error note."},
                    {"role": "user", "content": prompt}
                ]
            )
            return error_response['message'].get('content', '').strip() or fallback
        except Exception as e:
            logger.error(f"Error generating error message: {e}")
            return fallback

    async def load_history(self, channel_id, limit=None):
        if limit is None:
            limit = int(redis_client.get("tater:max_llm") or 8)

        history_key = f"tater:channel:{channel_id}:history"
        raw_history = redis_client.lrange(history_key, -limit, -1)
        loop_messages = []

        for entry in raw_history:
            data = json.loads(entry)
            role = data.get("role", "user")
            sender = data.get("username", role)
            content = data.get("content")

            # Only user/assistant roles are meaningful for the template
            if role not in ("user", "assistant"):
                role = "assistant"

            templ = _to_template_msg(role, content, sender=sender if role == "user" else None)
            if templ is not None:
                loop_messages.append(templ)

        # Merge consecutive same-role turns and ensure we start with 'user'
        return _enforce_user_assistant_alternation(loop_messages)

    async def save_message(self, channel_id, role, username, content):
        key = f"tater:channel:{channel_id}:history"
        max_store = int(redis_client.get("tater:max_store") or 20)
        redis_client.rpush(key, json.dumps({"role": role, "username": username, "content": content}))
        if max_store > 0:
            redis_client.ltrim(key, -max_store, -1)

    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return

        # Handle text + attachments
        if message.attachments:
            for attachment in message.attachments:
                try:
                    if not attachment.content_type:
                        continue

                    file_bytes = await attachment.read()
                    file_b64 = base64.b64encode(file_bytes).decode("utf-8")

                    if attachment.content_type.startswith("image/"):
                        file_type = "image"
                    elif attachment.content_type.startswith("audio/"):
                        file_type = "audio"
                    elif attachment.content_type.startswith("video/"):
                        file_type = "video"
                    else:
                        file_type = "file"

                    file_obj = {
                        "type": file_type,
                        "name": attachment.filename,
                        "mimetype": attachment.content_type,
                        "data": file_b64
                    }

                    await self.save_message(message.channel.id, "user", message.author.name, file_obj)
                except Exception as e:
                    logger.warning(f"Failed to store attachment ({attachment.filename}): {e}")
        else:
            # Just a text message
            await self.save_message(message.channel.id, "user", message.author.display_name, message.content)

        if isinstance(message.channel, discord.DMChannel):
            if message.author.id != self.admin_user_id:
                return
        else:
            if message.channel.id != self.response_channel_id and not self.user.mentioned_in(message):
                return

        system_prompt = self.build_system_prompt()
        history = await self.load_history(message.channel.id)
        messages_list = [{"role": "system", "content": system_prompt}] + history

        async with message.channel.typing():
            try:
                logger.debug(f"Sending messages to LLM: {messages_list}")
                response = await self.llm.chat(messages_list)
                response_text = response['message'].get('content', '').strip()
                if not response_text:
                    await message.channel.send("I'm not sure how to respond to that.")
                    return

                response_json = parse_function_json(response_text)

                if response_json:
                    func = response_json.get("function")
                    args = response_json.get("arguments", {})

                    # Save structured plugin_call marker
                    await self.save_message(
                        message.channel.id, "assistant", "assistant",
                        {"marker": "plugin_call", "plugin": func, "arguments": args}
                    )

                    if func in plugin_registry and get_plugin_enabled(func):
                        plugin = plugin_registry[func]

                        # Show waiting message if defined
                        if hasattr(plugin, "waiting_prompt_template"):
                            wait_msg = plugin.waiting_prompt_template.format(mention=message.author.mention)

                            # Directly fetch the waiting message text from LLM
                            wait_response = await self.llm.chat(
                                messages=[
                                    {"role": "system", "content": "Write one short, friendly status line."},
                                    {"role": "user", "content": wait_msg}
                                ]
                            )
                            wait_text = wait_response["message"]["content"].strip()

                            # Save waiting message to Redis
                            await self.save_message(
                                message.channel.id, "assistant", "assistant",
                                {"marker": "plugin_wait", "content": wait_text}
                            )

                            # Send waiting message to Discord
                            await safe_send(message.channel, wait_text, self.max_response_length)

                        result = await plugin.handle_discord(message, args, self.llm)

                        if isinstance(result, list):
                            for item in result:
                                if isinstance(item, str):
                                    await safe_send(message.channel, item, self.max_response_length)
                                    await self.save_message(
                                        message.channel.id, "assistant", "assistant",
                                        {"marker": "plugin_response", "phase": "final", "content": item}
                                    )

                                elif isinstance(item, dict):
                                    content_type = item.get("type")
                                    filename = item.get("name", "output.bin")

                                    try:
                                        if "bytes" in item:
                                            binary = item["bytes"]
                                        elif "data" in item:
                                            binary = base64.b64decode(item["data"])
                                        else:
                                            logger.warning(f"Missing 'bytes' or 'data' for {content_type} content.")
                                            continue

                                        file = discord.File(BytesIO(binary), filename=filename)
                                        await message.channel.send(file=file)

                                        content_obj = {
                                            "type": content_type,
                                            "name": filename,
                                            "mimetype": item.get("mimetype", ""),
                                            "data": base64.b64encode(binary).decode("utf-8")
                                        }

                                        await self.save_message(
                                            message.channel.id, "assistant", "assistant",
                                            {"marker": "plugin_response", "phase": "final", "content": content_obj}
                                        )

                                    except Exception as e:
                                        logger.warning(f"Failed to handle {content_type} return: {e}")

                        elif isinstance(result, str):
                            await safe_send(message.channel, result, self.max_response_length)
                            await self.save_message(
                                message.channel.id, "assistant", "assistant",
                                {"marker": "plugin_response", "phase": "final", "content": result}
                            )

                    else:
                        error = await self.generate_error_message(
                            f"Unknown or disabled function call: {func}.",
                            f"Function `{func}` is not available or disabled.",
                            message
                        )
                        await message.channel.send(error)
                        return

                else:
                    await safe_send(message.channel, response_text, self.max_response_length)
                    await self.save_message(message.channel.id, "assistant", "assistant", response_text)

            except Exception as e:
                logger.error(f"Exception in message handler: {e}")
                fallback = "An error occurred while processing your request."
                error_prompt = f"Generate a friendly error message to {message.author.mention} explaining that an error occurred while processing the request."
                error_msg = await self.generate_error_message(error_prompt, fallback, message)
                await message.channel.send(error_msg)


    async def on_reaction_add(self, reaction, user):
        if user.bot:
            return

        for plugin in plugin_registry.values():
            # Only call plugins that implement on_reaction_add
            if not hasattr(plugin, "on_reaction_add"):
                continue

            # Respect plugin toggle (even for passive ones)
            if not get_plugin_enabled(plugin.name):
                continue

            try:
                await plugin.on_reaction_add(reaction, user)
            except Exception as e:
                logger.error(f"[{plugin.name}] Error in on_reaction_add: {e}")

class AdminCommands(commands.Cog):
    def __init__(self, bot: discord_platform):
        self.bot = bot

    @app_commands.command(name="wipe", description="Clear chat history for this channel.")
    async def wipe(self, interaction: discord.Interaction):
        try:
            clear_channel_history(interaction.channel.id)
            await interaction.response.send_message("🧠 Wait What!?! What Just Happened!?!😭")
        except Exception as e:
            await interaction.response.send_message("Failed to clear channel history.")
            logger.error(f"Error in /wipe command: {e}")

async def setup_commands(client: commands.Bot):
    logger.info("Commands setup complete.")

def run(stop_event=None):
    token     = redis_client.hget("discord_platform_settings", "discord_token")
    admin_id  = redis_client.hget("discord_platform_settings", "admin_user_id")
    channel_id= redis_client.hget("discord_platform_settings", "response_channel_id")

    # Build LLM client
    llm_host = os.getenv("LLM_HOST", "127.0.0.1")
    llm_port = os.getenv("LLM_PORT", "11434")
    llm_client = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")

    if not (token and admin_id and channel_id):
        print("⚠️ Missing Discord settings in Redis. Bot not started.")
        return

    client = discord_platform(
        llm_client=llm_client,
        admin_user_id=int(admin_id),
        response_channel_id=int(channel_id),
        command_prefix="!",
        intents=discord.Intents.all()
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def run_bot():
        try:
            await client.start(token)
        except asyncio.CancelledError:
            # Expected on shutdown
            pass
        except Exception as e:
            print(f"❌ Discord bot crashed: {e}")

    def monitor_stop():
        # Only start monitor if a stop_event was provided
        if not stop_event:
            return
        while not stop_event.is_set():
            time.sleep(1)
        logger.info("🛑 Stop signal received for Discord platform. Logging out.")

        shutdown_complete = threading.Event()

        async def shutdown():
            try:
                await client.close()
                # discord.py keeps an aiohttp.ClientSession at client.http.session
                if hasattr(client, "http") and getattr(client.http, "session", None):
                    await client.http.session.close()
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error during Discord shutdown: {e}")
            finally:
                shutdown_complete.set()

        # Schedule shutdown coroutine on the bot’s loop
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(shutdown()))
        # Wait up to 15s for it to finish
        shutdown_complete.wait(timeout=15)

    # Start the monitor thread if we have a stop_event
    if stop_event:
        threading.Thread(target=monitor_stop, daemon=True).start()

    # Run the bot — ensures the loop is closed afterwards
    try:
        loop.run_until_complete(run_bot())
    finally:
        if not loop.is_closed():
            loop.close()