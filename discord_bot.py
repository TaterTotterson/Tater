# discord_bot.py
import asyncio
import threading
import os
import discord
from tater import tater  # your bot class
# Import our OllamaClientWrapper from helpers.py
from helpers import OllamaClientWrapper

# Global variables to store the event loop and task.
_bot_loop = None
_bot_task = None

def start_discord_bot(discord_token, admin_user_id, response_channel_id):
    global _bot_loop, _bot_task
    if _bot_task is None or _bot_task.done():
        _bot_loop = asyncio.new_event_loop()

        def run_loop(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, args=(_bot_loop,), daemon=True)
        thread.start()

        _bot_task = asyncio.run_coroutine_threadsafe(
            run_discord_bot(discord_token, admin_user_id, response_channel_id),
            _bot_loop
        )
        print("Discord bot started.")

async def run_discord_bot(discord_token, admin_user_id, response_channel_id):
    intents = discord.Intents.default()
    intents.message_content = True
    intents.reactions = True
    intents.dm_messages = True
    intents.dm_reactions = True

    ollama_host = os.getenv('OLLAMA_HOST', '127.0.0.1')
    ollama_port = int(os.getenv('OLLAMA_PORT', 11434))

    ollama_client = OllamaClientWrapper(host=f'http://{ollama_host}:{ollama_port}')
    
    client = tater(
        ollama_client=ollama_client,
        admin_user_id=admin_user_id,
        response_channel_id=response_channel_id,
        command_prefix="!",
        intents=intents
    )

    # Optional: inject the bot into plugin(s) here
    try:
        from plugins.discord_notifier import plugin as discord_plugin
        discord_plugin.set_bot(client)
    except Exception:
        pass

    await client.start(discord_token)

def stop_discord_bot():
    global _bot_loop, _bot_task
    if _bot_task is not None and not _bot_task.done():
        _bot_task.cancel()
        _bot_loop.call_soon_threadsafe(_bot_loop.stop)
        print("Discord bot stopped.")