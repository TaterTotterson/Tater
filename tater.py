# tater.py
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

# Import plugin registry
from plugin_registry import plugin_registry

load_dotenv()
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
max_response_length = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))

# Configure logging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('discord.tater')

# Initialize Redis client.
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# ------------------ HELPER FUNCTION FOR PLUGIN ENABLED STATE ------------------
def get_plugin_enabled(plugin_name):
    # Try to get the state from Redis; default to False (disabled) if not set.
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    if enabled is None:
        return False
    return enabled.lower() == "true"

def clear_channel_history(channel_id):
    """Clear chat history for the given channel only."""
    key = f"tater:channel:{channel_id}:history"
    try:
        redis_client.delete(key)
        logger.info(f"Cleared chat history for channel {channel_id}.")
    except Exception as e:
        logger.error(f"Error clearing chat history for channel {channel_id}: {e}")
        raise

from datetime import datetime

def build_system_prompt(base_prompt):
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
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        "If no function is needed, reply normally."
    )

BASE_PROMPT = (
    "You are Tater Totterson, a helpful AI assistant with access to various tools and plugins.\n\n"
    "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
)
SYSTEM_PROMPT = build_system_prompt(BASE_PROMPT)

class tater(commands.Bot):
    def __init__(self, ollama_client, admin_user_id, response_channel_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ollama = ollama_client  # This client now includes .model and .context_length defaults.
        self.admin_user_id = admin_user_id
        self.response_channel_id = response_channel_id
        self.max_response_length = max_response_length

    async def setup_hook(self):
        await setup_commands(self)
        # Load admin commands cog.
        await self.add_cog(AdminCommands(self))
        # Sync application commands.
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
            error_text = error_response['message'].get('content', '').strip()
            if error_text:
                return error_text
        except Exception as e:
            logger.error(f"Error generating error message: {e}")
        return fallback

    async def load_history(self, channel_id, limit=20):
        history_key = f"tater:channel:{channel_id}:history"
        raw_history = redis_client.lrange(history_key, -limit, -1)
        formatted_history = []
        for entry in raw_history:
            data = json.loads(entry)
            role = data.get("role", "user")
            sender = data.get("username", role)
            if role == "assistant":
                formatted_message = data["content"]
            else:
                formatted_message = f"{sender}: {data['content']}"
            formatted_history.append({"role": role, "content": formatted_message})
        return formatted_history

    async def save_message(self, channel_id, role, username, content):
        message_data = {"role": role, "username": username, "content": content}
        history_key = f"tater:channel:{channel_id}:history"
        redis_client.rpush(history_key, json.dumps(message_data))
        redis_client.ltrim(history_key, -20, -1)

    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return

        await self.save_message(message.channel.id, "user", message.author.name, message.content)

        if isinstance(message.channel, discord.DMChannel):
            if message.author.id == self.admin_user_id:
                should_respond = True
            else:
                return
        else:
            should_respond = (message.channel.id == self.response_channel_id or self.user.mentioned_in(message))
            if not should_respond:
                return

        system_prompt = SYSTEM_PROMPT
        recent_history = await self.load_history(message.channel.id, limit=20)
        messages_list = [{"role": "system", "content": system_prompt}] + recent_history

        async with message.channel.typing():
            try:
                logger.debug(f"Sending request to Ollama with messages: {messages_list}")
                response_data = await self.ollama.chat(
                    model=self.ollama.model,
                    messages=messages_list,
                    stream=False,
                    keep_alive=-1,
                    options={"num_ctx": self.ollama.context_length}
                )
                logger.debug(f"Raw response from Ollama: {response_data}")
                response_text = response_data['message'].get('content', '').strip()
                if not response_text:
                    logger.error("Ollama returned an empty response.")
                    await message.channel.send("I'm not sure how to respond to that.")
                    return

                try:
                    response_json = json.loads(response_text)
                except json.JSONDecodeError:
                    json_start = response_text.find('{')
                    json_end = response_text.rfind('}')
                    if json_start != -1 and json_end != -1:
                        json_str = response_text[json_start:json_end+1]
                        try:
                            response_json = json.loads(json_str)
                        except Exception:
                            response_json = None
                    else:
                        response_json = None

                if response_json and isinstance(response_json, dict) and "function" in response_json:
                    from plugin_registry import plugin_registry
                    func = response_json.get("function")
                    args = response_json.get("arguments", {})
                    # Only execute the plugin if it exists and is enabled.
                    if func in plugin_registry and get_plugin_enabled(func):
                        plugin = plugin_registry[func]
                        result = await plugin.handle_discord(
                            message, args, self.ollama, self.ollama.context_length, self.max_response_length
                        )
                        if result is not None and result != "":
                            response_text = result
                    else:
                        error_text = await self.generate_error_message(
                            f"Unknown or disabled function call: {func}.",
                            f"Received an unknown or disabled function call: {func}.",
                            message
                        )
                        await message.channel.send(error_text)
                        return
                else:
                    for chunk in [response_text[i:i+self.max_response_length] for i in range(0, len(response_text), self.max_response_length)]:
                        await message.channel.send(chunk)

                await self.save_message(message.channel.id, "assistant", "assistant", response_text)

            except Exception as e:
                logger.error(f"Exception occurred while processing message: {e}")
                error_prompt = f"Generate a friendly error message to {message.author.mention} explaining that an error occurred while processing the request."
                error_msg = await self.generate_error_message(error_prompt, "An error occurred while processing your request.", message)
                await message.channel.send(error_msg)

    async def on_reaction_add(self, reaction, user):
        if user.bot:
            return
        for plugin in plugin_registry.values():
            if hasattr(plugin, "on_reaction_add"):
                try:
                    await plugin.on_reaction_add(reaction, user)
                except Exception as e:
                    logger.error(f"[{plugin.name}] Error in on_reaction_add: {e}")

# Define a separate cog for admin commands, including /wipe.
class AdminCommands(commands.Cog):
    def __init__(self, bot: tater):
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
    print("Commands setup complete.")