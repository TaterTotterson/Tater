# discord_control.py
from discord_bot import start_discord_bot, stop_discord_bot

def connect_discord(discord_token, admin_user_id, response_channel_id):
    start_discord_bot(discord_token, admin_user_id, response_channel_id)

def disconnect_discord():
    stop_discord_bot()