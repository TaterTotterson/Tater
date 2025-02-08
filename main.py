import asyncio
import os
import dotenv
import ollama
import discord
from discord.ext import commands
from tater import tater, clear_redis
import YouTube

dotenv.load_dotenv()

# Load environment variables
discord_token = os.getenv('DISCORD_TOKEN')
ollama_host = os.getenv('OLLAMA_HOST', '127.0.0.1')
ollama_port = int(os.getenv('OLLAMA_PORT', 11434))

async def main():
    intents = discord.Intents.default()
    intents.message_content = True

    ollama_client = ollama.AsyncClient(host=f'http://{ollama_host}:{ollama_port}')

    client = tater(
        ollama_client=ollama_client,
        command_prefix="!",
        intents=intents
    )

    @client.event
    async def on_ready():
        print(f'Tater is ready. Logged in as {client.user} (ID: {client.user.id})')
        if client.application_id is None:
            print("Application ID not set. Trying to access it automatically.")
            client.application_id = client.user.id
        print(f'Application ID: {client.application_id}')
        try:
            await client.tree.sync()
            print("Slash commands synced.")
        except Exception as e:
            print(f"Failed to sync slash commands: {e}")

    @client.tree.command(name="wipe", description="Wipe Tater's Mind")
    async def wipe_command(interaction: discord.Interaction):
        try:
            clear_redis()
            # Announce to everyone in the channel
            await interaction.response.send_message("Where am I?!? What happened?!?")
        except Exception as e:
            # Announce the error publicly in the channel
            await interaction.response.send_message(f"An error occurred while clearing Redis: {e}")

    await client.start(discord_token)

if __name__ == '__main__':
    asyncio.run(main())