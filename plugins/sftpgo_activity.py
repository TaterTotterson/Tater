# plugins/sftpgo_activity.py
import os
import asyncio
import discord
import aiohttp
import base64
import redis
import secrets
import string
from plugin_base import ToolPlugin
from helpers import load_image_from_url, format_irc
from chat_helpers import send_waiting_message, save_assistant_message
import streamlit as st

class SFTPGoActivityPlugin(ToolPlugin):
    name = "sftpgo_activity"
    usage = (
        '{\n'
        '  "function": "sftpgo_activity",\n'
        '  "arguments": {} \n'
        '}\n'
    )
    description = ("Retrieves current connection activity from the SFTPGo server and provides a friendly summary.")
    settings_category = "SFTPGo"
    required_settings = {
        "SFTPGO_API_URL": {
            "label": "SFTPGo API URL",
            "type": "text",
            "default": "https://localhost",
            "description": "Enter the base URL for the SFTPGo API (do not include /api/v2)."
        },
        "SFTPGO_USERNAME": {
            "label": "SFTPGo Username",
            "type": "text",
            "default": "username",
            "description": "The username to authenticate with the SFTPGo API."
        },
        "SFTPGO_PASSWORD": {
            "label": "SFTPGo Password",
            "type": "password",
            "default": "password",
            "description": "The password to authenticate with the SFTPGo API."
        }
    }
    waiting_prompt_template = "Generate a brief message to {mention} telling them to wait a moment while you access the server to see who is using it. Only generate the message. Do not respond to this message."
    platforms = ["discord", "webui"]

    def get_sftpgo_settings(self):
        """
        Retrieves SFTPGo settings from Redis for the 'SFTPGo' settings category.
        Fallback defaults are used if settings are missing.
        Appends '/api/v2' if not present.
        """
        redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        key = "plugin_settings:SFTPGo"
        settings = redis_client.hgetall(key)
        defaults = {
            "SFTPGO_API_URL": "https://localhost",
            "SFTPGO_USERNAME": "username",
            "SFTPGO_PASSWORD": "password"
        }
        for k, default_value in defaults.items():
            if k not in settings or not settings[k]:
                settings[k] = default_value

        api_url = settings["SFTPGO_API_URL"].rstrip("/")
        if "/api/v2" not in api_url:
            api_url += "/api/v2"
        settings["SFTPGO_API_URL"] = api_url
        return settings

    async def get_jwt_token(self):
        """Obtain a JWT token from the SFTPGo API."""
        settings = self.get_sftpgo_settings()
        auth_header = base64.b64encode(f"{settings['SFTPGO_USERNAME']}:{settings['SFTPGO_PASSWORD']}".encode("utf-8")).decode("ascii")
        connector = aiohttp.TCPConnector(ssl=False)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    f"{settings['SFTPGO_API_URL']}/token",
                    headers={"Authorization": f"Basic {auth_header}"}
                ) as response:
                    if response.status == 200:
                        json_response = await response.json()
                        return json_response.get("access_token")
                    else:
                        print(f"Failed to obtain JWT token. Status code: {response.status}")
                        return None
        except Exception as e:
            print(f"Error obtaining JWT token: {e}")
            return None

    async def get_current_activity_raw(self, message_obj):
        """
        Retrieves raw connection details from SFTPGo using the /connections endpoint.
        Returns a text summary.
        """
        settings = self.get_sftpgo_settings()
        jwt_token = await self.get_jwt_token()
        connector = aiohttp.TCPConnector(ssl=False)
        if jwt_token is None:
            return "Failed to obtain JWT token."
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                f"{settings['SFTPGO_API_URL']}/connections",
                headers={"Authorization": f"Bearer {jwt_token}"}
            ) as response:
                if response.status == 200:
                    connections = await response.json()
                    if not connections:
                        return "No active connections."
                    message_lines = ["Active Connections:"]
                    for conn in connections:
                        username = conn.get("username", "unknown")
                        client_version = conn.get("client_version", "unknown")
                        connection_time = conn.get("connection_time", "unknown")
                        command = conn.get("command", "unknown")
                        last_activity = conn.get("last_activity", "unknown")
                        protocol = conn.get("protocol", "unknown")
                        active_transfers = conn.get("active_transfers", [])
                        transfers_info = ""
                        if active_transfers:
                            transfers_lines = []
                            for transfer in active_transfers:
                                operation_type = transfer.get("operation_type", "unknown")
                                path = transfer.get("path", "unknown")
                                start_time = transfer.get("start_time", "unknown")
                                size = transfer.get("size", "unknown")
                                transfers_lines.append(f"{operation_type} {path} (start: {start_time}, size: {size})")
                            transfers_info = "\n    Active Transfers: " + "; ".join(transfers_lines)
                        message_lines.append(
                            f"User: {username}, Client: {client_version}, Connected: {connection_time}, "
                            f"Command: {command}, Last Activity: {last_activity}, Protocol: {protocol}{transfers_info}"
                        )
                    return "\n".join(message_lines)
                else:
                    error_text = await response.text()
                    return f"Failed to retrieve connections info. Status code: {response.status}, Error: {error_text}"

    async def get_current_activity(self, message_obj, ollama_client):
        """
        Uses raw connection details and Ollama to generate a friendly summary.
        """
        raw_activity = await self.get_current_activity_raw(message_obj)
        prompt = (
            f"The following are the current connection details from the server:\n\n{raw_activity}\n\n"
            "Please provide a brief status report of the current connection status on the server. Only generate the message. Do not respond to this message."
        )
        response_data = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "user", "content": prompt}]
        )
        response_text = response_data["message"].get("content", "")
        if len(response_text) > 4000:
            response_text = response_text[:3990] + " [truncated]"
        return response_text

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):

        user = message.author
        waiting_prompt = self.waiting_prompt_template.format(mention=user.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: asyncio.create_task(save_assistant_message(message.channel.id, text)),
            send_callback=lambda text: message.channel.send(text)
        )

        result = await self.get_current_activity(message, ollama_client)
        await message.channel.send(result)
        await save_assistant_message(message.channel.id, result)
        return ""


    async def handle_webui(self, args, ollama_client, context_length):

        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: asyncio.create_task(save_assistant_message("webui", text)),
            send_callback=lambda text: st.chat_message("assistant", avatar=load_image_from_url()).write(text)
        )

        result = await self.get_current_activity(None, ollama_client)
        await save_assistant_message("webui", result)
        return result

    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        mention = user
        waiting_prompt = self.waiting_prompt_template.format(mention=mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: asyncio.create_task(save_assistant_message(channel, f"{mention}: {text}")),
            send_callback=lambda text: bot.privmsg(channel, f"{mention}: {text}")
        )
        result = await self.get_current_activity(None, ollama_client)
        formatted = format_irc(result)
        for chunk in [formatted[i:i + 400] for i in range(0, len(formatted), 400)]:
            await bot.privmsg(channel, chunk)
            await save_assistant_message(channel, chunk)

# Export an instance of the plugin.
plugin = SFTPGoActivityPlugin()