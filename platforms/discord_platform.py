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

BASE_PROMPT = (
    "You are Tater Totterson, a Discord-savvy AI assistant with access to various tools and plugins.\n\n"
    "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
)

class discord_platform(commands.Bot):
    def __init__(self, ollama_client, admin_user_id, response_channel_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ollama = ollama_client
        self.admin_user_id = admin_user_id
        self.response_channel_id = response_channel_id
        self.max_response_length = max_response_length

    def build_system_prompt(self):
        now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
        tool_instructions = "\n\n".join(
            f"Tool: {plugin.name}\n"
            f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
            f"{plugin.usage}"
            for plugin in plugin_registry.values()
            if ("discord" in plugin.platforms or "both" in plugin.platforms) and get_plugin_enabled(plugin.name)
        )
        return (
            f"Current Date and Time is: {now}\n\n"
            f"{BASE_PROMPT}\n\n"
            f"{tool_instructions}\n\n"
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
                model=self.ollama.model,
                messages=[{"role": "system", "content": prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": self.ollama.context_length}
            )
            return error_response['message'].get('content', '').strip() or fallback
        except Exception as e:
            logger.error(f"Error generating error message: {e}")
            return fallback

    async def load_history(self, channel_id, limit=20):
        history_key = f"tater:channel:{channel_id}:history"
        raw_history = redis_client.lrange(history_key, -limit, -1)
        formatted = []
        for entry in raw_history:
            data = json.loads(entry)
            role = data.get("role", "user")
            sender = data.get("username", role)
            formatted.append({
                "role": role,
                "content": data["content"] if role == "assistant" else f"{sender}: {data['content']}"
            })
        return formatted

    async def save_message(self, channel_id, role, username, content):
        key = f"tater:channel:{channel_id}:history"
        redis_client.rpush(key, json.dumps({"role": role, "username": username, "content": content}))
        redis_client.ltrim(key, -20, -1)

    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return

        await self.save_message(message.channel.id, "user", message.author.name, message.content)

        if isinstance(message.channel, discord.DMChannel):
            if message.author.id != self.admin_user_id:
                return
        else:
            if message.channel.id != self.response_channel_id and not self.user.mentioned_in(message):
                return

        system_prompt = self.build_system_prompt()
        history = await self.load_history(message.channel.id, limit=20)
        messages_list = [{"role": "system", "content": system_prompt}] + history

        async with message.channel.typing():
            try:
                logger.debug(f"Sending messages to Ollama: {messages_list}")
                response = await self.ollama.chat(
                    model=self.ollama.model,
                    messages=messages_list,
                    stream=False,
                    keep_alive=-1,
                    options={"num_ctx": self.ollama.context_length}
                )
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
                        result = await plugin.handle_discord(
                            message, args, self.ollama, self.ollama.context_length, self.max_response_length
                        )
                        if isinstance(result, str) and result.strip():
                            await safe_send(message.channel, result, self.max_response_length)
                            await self.save_message(message.channel.id, "assistant", "assistant", result)
                        else:
                            logger.debug(f"[{func}] Plugin returned no usable response (type: {type(result)}).")
                        return  # ✅ Prevent fallback
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
            if hasattr(plugin, "on_reaction_add") and get_plugin_enabled(plugin.name):
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

def run():
    # Load settings from Redis
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', '127.0.0.1'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

    token = redis_client.hget("discord_platform_settings", "discord_token")
    admin_id = redis_client.hget("discord_platform_settings", "admin_user_id")
    channel_id = redis_client.hget("discord_platform_settings", "response_channel_id")

    # ✅ Correct Ollama setup
    ollama_host = os.getenv("OLLAMA_HOST", "127.0.0.1")
    ollama_port = os.getenv("OLLAMA_PORT", "11434")
    ollama_client = OllamaClientWrapper(host=f"http://{ollama_host}:{ollama_port}")

    if token and admin_id and channel_id:
        client = discord_platform(
            ollama_client=ollama_client,
            admin_user_id=int(admin_id),
            response_channel_id=int(channel_id),
            command_prefix="!",
            intents=discord.Intents.all()
        )
        client.run(token)
    else:
        print("⚠️ Missing Discord settings in Redis. Bot not started.")