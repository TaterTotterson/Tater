# plugins/sftpgo_activity.py
import os
import asyncio
import aiohttp
import base64
import redis
from plugin_base import ToolPlugin

class SFTPGoActivityPlugin(ToolPlugin):
    name = "sftpgo_activity"
    plugin_name = "SFTPGo Activity"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        '{\n'
        '  "function": "sftpgo_activity",\n'
        '  "arguments": {} \n'
        '}\n'
    )
    description = "Retrieves current connection activity from the SFTPGo server."
    plugin_dec = "Show current connection activity on the SFTPGo server."
    pretty_name = "Checking FTP Activity"
    settings_category = "SFTPGo"
    waiting_prompt_template = "Write a friendly message telling {mention} you’re accessing the server to see who’s using it now! Only output that message."
    platforms = ["discord", "webui", "irc", "matrix"]
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

    def get_sftpgo_settings(self):
        redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        key = f"plugin_settings:{self.settings_category}"
        settings = client.hgetall(key)
        defaults = {
            "SFTPGO_API_URL": "https://localhost",
            "SFTPGO_USERNAME": "username",
            "SFTPGO_PASSWORD": "password"
        }
        for k, default in defaults.items():
            if not settings.get(k):
                settings[k] = default
        if "/api/v2" not in settings["SFTPGO_API_URL"]:
            settings["SFTPGO_API_URL"] = settings["SFTPGO_API_URL"].rstrip("/") + "/api/v2"
        return settings

    async def get_jwt_token(self, settings):
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
                        return (await response.json()).get("access_token")
        except Exception as e:
            print(f"JWT error: {e}")
        return None

    async def _get_activity_summary(self, llm_client):
        settings = self.get_sftpgo_settings()
        jwt_token = await self.get_jwt_token(settings)
        if not jwt_token:
            return "❌ Failed to obtain JWT token."

        connector = aiohttp.TCPConnector(ssl=False)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    f"{settings['SFTPGO_API_URL']}/connections",
                    headers={"Authorization": f"Bearer {jwt_token}"}
                ) as resp:
                    if resp.status != 200:
                        return f"❌ Failed to retrieve connections: {resp.status}"

                    conns = await resp.json()
                    if not conns:
                        return "No active connections."

                    lines = ["Active Connections:"]
                    for c in conns:
                        lines.append(
                            f"User: {c.get('username')}, "
                            f"Client: {c.get('client_version')}, "
                            f"Protocol: {c.get('protocol')}, "
                            f"Last Activity: {c.get('last_activity')}"
                        )
                    joined = "\n".join(lines)

                    prompt = (
                        f"The following are the current connection details from the server:\n\n{joined}\n\n"
                        "Please provide a brief status report of the current connection status on the server. "
                        "Only generate the message. Do not respond to this message."
                    )

                    response = await llm_client.chat(
                        messages=[{"role": "user", "content": prompt}]
                    )
                    return response["message"].get("content", "").strip() or joined

        except Exception as e:
            return f"❌ Error retrieving activity: {e}"

    # Discord
    async def handle_discord(self, message, args, llm_client):
        return await self._get_activity_summary(llm_client)

    # IRC (return string; the platform will send it)
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        msg = await self._get_activity_summary(llm_client)
        return f"{user}: {msg}"

    # WebUI
    async def handle_webui(self, args, llm_client):
        try:
            asyncio.get_running_loop()
            return [await self._get_activity_summary(llm_client)]
        except RuntimeError:
            return [asyncio.run(self._get_activity_summary(llm_client))]

    # Matrix
    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        """
        Return plain text; Matrix platform will post it (and chunk if needed).
        """
        return await self._get_activity_summary(llm_client)


plugin = SFTPGoActivityPlugin()
