# main.py
import asyncio
import os
import dotenv
import ollama
import discord
from discord.ext import commands
from tater import tater, clear_redis
import rss  # Import the new module
import YouTube

dotenv.load_dotenv()

discord_token = os.getenv('DISCORD_TOKEN')
ollama_host = os.getenv('OLLAMA_HOST', '127.0.0.1')
ollama_port = int(os.getenv('OLLAMA_PORT', 11434))
admin_user_id = int(os.getenv("ADMIN_USER_ID", 0))

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

    @client.tree.command(name="wipe", description="Wipe Tater's Mind (Admin Only)")
    async def wipe_command(interaction: discord.Interaction):
        # Check if the user is the authorized admin
        if interaction.user.id != admin_user_id:
            await interaction.response.send_message("❌ You do not have permission to use this command!", ephemeral=True)
            return

        try:
            clear_redis()
            await interaction.response.send_message("🧠 Where am I?!? What happened?!?")
        except Exception as e:
            await interaction.response.send_message(f"❌ An error occurred while clearing Redis: {e}")

    # Initialize the RSS Manager and attach it to the bot.
    client.rss_manager = rss.setup_rss_manager(client)

    await client.start(discord_token)

if __name__ == '__main__':
    asyncio.run(main())