# discord_platform.py
import os
import json
import asyncio
import logging
import redis
import discord
from discord.ext import commands
from discord import app_commands
import ollama
from dotenv import load_dotenv
import re
from datetime import datetime
from plugin_registry import plugin_registry
from helpers import OllamaClientWrapper, parse_function_json
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
            "description": "Channel where Tater replies"
        }
    }
}

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
    def __init__(self, ollama_client, admin_user_id, response_channel_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ollama = ollama_client
        self.admin_user_id = admin_user_id
        self.response_channel_id = response_channel_id
        self.max_response_length = max_response_length

    def build_system_prompt(self):
        now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

        base_prompt = (
            "You are Tater Totterson, a Discord-savvy AI assistant with access to various tools and plugins.\n\n"
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

    async def setup_hook(self):
        await self.add_cog(AdminCommands(self))
        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} app commands.")
        except Exception as e:
            logger.error(f"Failed to sync app commands: {e}")

    async def on_ready(self):
        activity = discord.Activity(name='tater', state='Totterson', type=discord.ActivityType.custom)
        await self.change_presence(activity=activity)
        logger.info(f"Bot is ready. Admin: {self.admin_user_id}, Response Channel: {self.response_channel_id}")

    async def generate_error_message(self, prompt: str, fallback: str, message: discord.Message):
        try:
            error_response = await self.ollama.chat(
                messages=[{"role": "system", "content": prompt}]
            )
            return error_response['message'].get('content', '').strip() or fallback
        except Exception as e:
            logger.error(f"Error generating error message: {e}")
            return fallback

    async def load_history(self, channel_id, limit=None):
        if limit is None:
            limit = int(redis_client.get("tater:max_ollama") or 8)

        history_key = f"tater:channel:{channel_id}:history"
        raw_history = redis_client.lrange(history_key, -limit, -1)
        formatted = []

        for entry in raw_history:
            data = json.loads(entry)
            role = data.get("role", "user")
            sender = data.get("username", role)
            content = data.get("content")

            if isinstance(content, dict):
                file_type = content.get("type")
                name = content.get("name", "unnamed file")

                if file_type == "image":
                    placeholder = f"[Image: {name}]"
                elif file_type == "audio":
                    placeholder = f"[Audio: {name}]"
                elif file_type == "video":
                    placeholder = f"[Video: {name}]"
                elif file_type == "file":
                    placeholder = f"[File: {name}]"
                else:
                    continue  # skip unknown or unsupported types
            elif isinstance(content, str):
                placeholder = content
            else:
                continue

            formatted.append({
                "role": role,
                "content": placeholder if role == "assistant" else f"{sender}: {placeholder}"
            })

        return formatted

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
                logger.debug(f"Sending messages to Ollama: {messages_list}")
                response = await self.ollama.chat(messages_list)
                response_text = response['message'].get('content', '').strip()
                if not response_text:
                    await message.channel.send("I'm not sure how to respond to that.")
                    return

                response_json = parse_function_json(response_text)

                if response_json:
                    func = response_json.get("function")
                    args = response_json.get("arguments", {})

                    if func in plugin_registry and get_plugin_enabled(func):
                        plugin = plugin_registry[func]

                        # Show waiting message if defined
                        if hasattr(plugin, "waiting_prompt_template"):
                            wait_msg = plugin.waiting_prompt_template.format(mention=message.author.mention)

                            # Directly fetch the waiting message text from Ollama
                            wait_response = await self.ollama.chat(
                                messages=[{"role": "system", "content": wait_msg}]
                            )
                            wait_text = wait_response["message"]["content"].strip()

                            # Save waiting message to Redis
                            await self.save_message(message.channel.id, "assistant", "assistant", wait_text)

                            # Send waiting message to Discord
                            await safe_send(message.channel, wait_text, self.max_response_length)

                        result = await plugin.handle_discord(message, args, self.ollama)

                        if isinstance(result, list):
                            for item in result:
                                if isinstance(item, str):
                                    await safe_send(message.channel, item, self.max_response_length)
                                    await self.save_message(message.channel.id, "assistant", "assistant", item)

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

                                        # Save full image/audio content to Redis
                                        content_obj = {
                                            "type": content_type,
                                            "name": filename,
                                            "mimetype": item.get("mimetype", ""),
                                            "data": base64.b64encode(binary).decode("utf-8")
                                        }
                                        await self.save_message(message.channel.id, "assistant", "assistant", content_obj)

                                    except Exception as e:
                                        logger.warning(f"Failed to handle {content_type} return: {e}")

                        elif isinstance(result, str):
                            await safe_send(message.channel, result, self.max_response_length)
                            await self.save_message(message.channel.id, "assistant", "assistant", result)

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

    # Build Ollama client
    ollama_host = os.getenv("OLLAMA_HOST", "127.0.0.1")
    ollama_port = os.getenv("OLLAMA_PORT", "11434")
    ollama_client = OllamaClientWrapper(host=f"http://{ollama_host}:{ollama_port}")

    if not (token and admin_id and channel_id):
        print("⚠️ Missing Discord settings in Redis. Bot not started.")
        return

    client = discord_platform(
        ollama_client=ollama_client,
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