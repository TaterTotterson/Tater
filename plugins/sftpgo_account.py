# plugins/sftpgo_account.py
import os
import asyncio
import discord
import aiohttp
import base64
import redis
import secrets
import string
from plugin_base import ToolPlugin

class SFTPGoAccountPlugin(ToolPlugin):
    name = "sftpgo_account"
    usage = (
        '{\n'
        '  "function": "sftpgo_account",\n'
        '  "arguments": { }\n'
        '}\n'
    )
    description = ("Creates an SFTPGo account on the server for the user.")
    pretty_name = "Creating Account"
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
        },
        "SFTPGO_GROUP_NAME": {
            "label": "SFTPGo Group Name",
            "type": "text",
            "default": "DNServ",
            "description": "Enter the group name to assign to new SFTP accounts."
        },
        "DEFAULT_HOME_DIR": {
            "label": "Default Home Directory",
            "type": "text",
            "default": "/your/default/home/dir",
            "description": "The default home directory for new SFTP accounts."
        }
    }
    waiting_prompt_template = "Write a friendly message telling {mention} you’re creating their account now! Only output that message."
    platforms = ["discord", "irc"]

    async def safe_send(channel, content: str, **kwargs):
        if len(content) > 2000:
            content = content[:1997] + "..."
        await channel.send(content, **kwargs)

    def get_sftpgo_settings(self):
        """
        Retrieves SFTPGo settings from Redis for the 'SFTPGo' settings category.
        Fallback defaults are used if settings are missing.
        If the provided API URL does not contain "/api/v2", it is appended.
        """
        redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        key = "plugin_settings:SFTPGo"
        settings = redis_client.hgetall(key)
        defaults = {
            "SFTPGO_API_URL": "https://localhost",
            "SFTPGO_USERNAME": "username",
            "SFTPGO_PASSWORD": "password",
            "SFTPGO_GROUP_NAME": "DNServ",
            "DEFAULT_HOME_DIR": ""
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
        auth_header = base64.b64encode(
            f"{settings['SFTPGO_USERNAME']}:{settings['SFTPGO_PASSWORD']}".encode("utf-8")
        ).decode("ascii")
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
            print(f"An error occurred while obtaining JWT token: {e}")
            return None

    def generate_random_password(self, length=12):
        """Generate a secure random password of given length."""
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return ''.join(secrets.choice(alphabet) for _ in range(length))

    async def create_sftp_account(self, username, password, message_obj):
        settings = self.get_sftpgo_settings()
        jwt_token = await self.get_jwt_token()
        connector = aiohttp.TCPConnector(ssl=False)  # Always disable SSL verification.

        if jwt_token is None:
            await safe_send(message_obj.channel, "Failed to obtain JWT token.")
            return "token_error"

        async with aiohttp.ClientSession(connector=connector) as session:
            # Check if the user already exists.
            async with session.get(
                f"{settings['SFTPGO_API_URL']}/users/{username}",
                headers={"Authorization": f"Bearer {jwt_token}"}
            ) as user_check_response:
                if user_check_response.status == 200:
                    return "exists"

            # Create the new user account.
            async with session.post(
                f"{settings['SFTPGO_API_URL']}/users",
                headers={"Authorization": f"Bearer {jwt_token}"},
                json={
                    "username": username,
                    "password": password,
                    "status": 1,
                    "permissions": {"/": ["list", "download", "upload", "create_dirs", "rename"]},
                    "home_dir": settings["DEFAULT_HOME_DIR"],
                    "groups": [{"name": settings["SFTPGO_GROUP_NAME"], "type": 1}]
                }
            ) as response:
                if response.status == 201:
                    welcome_message = (
                        f"Welcome '{username}'\n"
                        f"Your account has been created.\n"
                        f"Login: {username}\n"
                        f"Password: {password}\n"
                        "You now have access to the server."
                    )
                    try:
                        await message_obj.author.send(welcome_message)
                    except Exception as e:
                        print(f"Failed to send DM to {username}: {e}")
                    return "created"
                else:
                    error_text = await response.text()
                    await safe_send(message_obj.channel, f"Failed to create user. Status code: {response.status}, Error: {error_text}")
                    return "error"

    # --- Discord Handler ---
    async def handle_discord(self, message, args, llm_client):
        user = message.author
        password = self.generate_random_password()

        result = await self.create_sftp_account(user.name, password, message)

        if result == "created":
            prompt = f"Generate a brief message stating that an account for '{user.name}' has been successfully created."
        elif result == "exists":
            prompt = f"Generate a brief message stating that '{user.name}' already has an account but is trying to make a new one!"
        else:
            prompt = f"Generate a brief message stating that an error occurred while creating the account for '{user.name}'."

        response_data = await llm_client.chat(
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response_data['message'].get('content', '').strip()
        if password:
            response_text += f"\n\n🔐 Password: `{password}`"
        return response_text


    # --- WebUI Handler ---
    async def handle_webui(self, args, llm_client):
        return "❌ SFTPGo account creation is not supported on the web UI."


    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        password = self.generate_random_password()
        result = await self.create_sftp_account(user, password, None)

        if result == "created":
            prompt = f"Generate a brief message stating that an account for '{user}' has been successfully created."
        elif result == "exists":
            prompt = f"Generate a brief message stating that '{user}' already has an account but is trying to make a new one!"
        else:
            prompt = f"Generate a brief message stating that an error occurred while creating the account for '{user}'."

        response_data = await llm_client.chat(messages=[{"role": "user", "content": prompt}])
        response_text = response_data['message'].get("content", "").strip()
        if password:
            # keep it on the same paragraph so the platform sends it as one message
            response_text += f" Password: {password}"

        return f"{user}: {response_text}"

plugin = SFTPGoAccountPlugin()