# plugins/sftpgo_account.py
import os
import aiohttp
import base64
import redis
import secrets
import string
import logging
from plugin_base import ToolPlugin

# Discord types are optional; import lazily in the Discord handler if needed
try:
    import discord
except Exception:
    discord = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SFTPGoAccountPlugin(ToolPlugin):
    name = "sftpgo_account"
    plugin_name = "SFTPGo Account"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        '{\n'
        '  "function": "sftpgo_account",\n'
        '  "arguments": { "username": "<optional custom username>" }\n'
        '}\n'
    )
    description = "Creates an SFTPGo account for the user and returns their credentials."
    plugin_dec = "Create an SFTPGo account for the user and return login details."
    pretty_name = "Creating Account"
    settings_category = "SFTPGo"
    required_settings = {
        "SFTPGO_API_URL": {
            "label": "SFTPGo API URL",
            "type": "text",
            "default": "https://localhost",
            "description": "Base URL for the SFTPGo API (do not include /api/v2)."
        },
        "SFTPGO_USERNAME": {
            "label": "SFTPGo Username",
            "type": "text",
            "default": "username",
            "description": "Username to authenticate with the SFTPGo API."
        },
        "SFTPGO_PASSWORD": {
            "label": "SFTPGo Password",
            "type": "password",
            "default": "password",
            "description": "Password to authenticate with the SFTPGo API."
        },
        "SFTPGO_GROUP_NAME": {
            "label": "SFTPGo Group Name",
            "type": "text",
            "default": "DNServ",
            "description": "Group name to assign to new SFTP accounts."
        },
        "DEFAULT_HOME_DIR": {
            "label": "Default Home Directory",
            "type": "text",
            "default": "/your/default/home/dir",
            "description": "Default home directory for new SFTP accounts."
        }
    }
    waiting_prompt_template = "Write a friendly message telling {mention} you’re creating their account now! Only output that message."
    platforms = ["discord", "webui", "irc", "matrix"]

    @staticmethod
    async def safe_send(channel, content: str, **kwargs):
        if not channel:
            return
        if len(content) > 2000:
            content = content[:1997] + "..."
        await channel.send(content, **kwargs)

    @staticmethod
    def _redis():
        redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        return redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

    def get_sftpgo_settings(self):
        """
        Get SFTPGo settings from Redis. Append '/api/v2' to API URL if missing.
        """
        rc = self._redis()
        settings = rc.hgetall("plugin_settings:SFTPGo") or {}
        defaults = {
            "SFTPGO_API_URL": "https://localhost",
            "SFTPGO_USERNAME": "username",
            "SFTPGO_PASSWORD": "password",
            "SFTPGO_GROUP_NAME": "DNServ",
            "DEFAULT_HOME_DIR": "/your/default/home/dir",
        }
        for k, v in defaults.items():
            if not settings.get(k):
                settings[k] = v

        api_url = settings["SFTPGO_API_URL"].rstrip("/")
        if "/api/v2" not in api_url:
            api_url += "/api/v2"
        settings["SFTPGO_API_URL"] = api_url
        return settings

    async def get_jwt_token(self):
        """Obtain a JWT token from the SFTPGo API."""
        s = self.get_sftpgo_settings()
        auth_header = base64.b64encode(f"{s['SFTPGO_USERNAME']}:{s['SFTPGO_PASSWORD']}".encode("utf-8")).decode("ascii")
        connector = aiohttp.TCPConnector(ssl=False)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    f"{s['SFTPGO_API_URL']}/token",
                    headers={"Authorization": f"Basic {auth_header}"}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("access_token")
                    logger.error(f"SFTPGo token error HTTP {resp.status}")
        except Exception as e:
            logger.exception(f"SFTPGo token exception: {e}")
        return None

    @staticmethod
    def generate_random_password(length=14):
        """Generate a secure random password."""
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return "".join(secrets.choice(alphabet) for _ in range(length))

    @staticmethod
    def _sanitize_username(raw: str) -> str:
        """
        Restrict to a safe subset for SFTPGo. Replace anything else with '_'.
        Limit length to 32 chars.
        """
        raw = (raw or "").strip()
        safe = []
        for ch in raw:
            if ch.isalnum() or ch in ("-", "_", "."):
                safe.append(ch)
            else:
                safe.append("_")
        out = "".join(safe).strip("._")  # avoid leading/trailing dots
        return (out or "user")[:32]

    @staticmethod
    def _matrix_localpart(sender_mxid: str) -> str:
        """
        Extract localpart from a Matrix user ID like '@alice:example.org' -> 'alice'.
        """
        if not sender_mxid:
            return "user"
        if sender_mxid.startswith("@"):
            return sender_mxid[1:].split(":", 1)[0] or "user"
        return sender_mxid

    async def create_sftp_account(self, username: str, password: str, message_obj=None):
        """
        Create the SFTPGo user. Returns ('created', welcome_text) on success;
        otherwise ('exists'|'error'|'token_error', None).
        """
        settings = self.get_sftpgo_settings()
        jwt = await self.get_jwt_token()
        if jwt is None:
            try:
                await self.safe_send(getattr(message_obj, "channel", None), "Failed to obtain JWT token.")
            except Exception:
                pass
            return "token_error", None

        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Check if exists
            try:
                async with session.get(
                    f"{settings['SFTPGO_API_URL']}/users/{username}",
                    headers={"Authorization": f"Bearer {jwt}"}
                ) as chk:
                    if chk.status == 200:
                        return "exists", None
            except Exception as e:
                logger.exception(f"SFTPGo user check error: {e}")
                return "error", None

            # Create user
            payload = {
                "username": username,
                "password": password,
                "status": 1,
                "permissions": {"/": ["list", "download", "upload", "create_dirs", "rename"]},
                "home_dir": settings["DEFAULT_HOME_DIR"],
                "groups": [{"name": settings["SFTPGO_GROUP_NAME"], "type": 1}],
            }
            try:
                async with session.post(
                    f"{settings['SFTPGO_API_URL']}/users",
                    headers={"Authorization": f"Bearer {jwt}"},
                    json=payload
                ) as resp:
                    if resp.status == 201:
                        welcome = (
                            f"Welcome '{username}'\n"
                            f"Your SFTP account has been created.\n"
                            f"Login: {username}\n"
                            f"Password: {password}\n"
                            "You now have access to the server."
                        )
                        # Discord DM if possible
                        try:
                            author = getattr(message_obj, "author", None)
                            if author and hasattr(author, "send"):
                                await author.send(welcome)
                        except Exception as e:
                            logger.info(f"Unable to DM credentials to {username}: {e}")
                        return "created", welcome
                    else:
                        errtxt = await resp.text()
                        logger.error(f"SFTPGo create error HTTP {resp.status}: {errtxt}")
                        try:
                            await self.safe_send(getattr(message_obj, "channel", None),
                                                 f"Failed to create user. HTTP {resp.status}")
                        except Exception:
                            pass
                        return "error", None
            except Exception as e:
                logger.exception(f"SFTPGo create exception: {e}")
                return "error", None

    # ---------------- Platform handlers ----------------

    async def handle_discord(self, message, args, llm_client):
        desired = (args or {}).get("username")
        username = (desired or getattr(message.author, "name", "user")).strip()
        username = self._sanitize_username(username)
        password = self.generate_random_password()

        state, _welcome = await self.create_sftp_account(username, password, message)

        if state == "created":
            prompt = f"Generate a brief message stating that an account for '{username}' has been successfully created."
        elif state == "exists":
            prompt = f"Generate a brief message stating that '{username}' already has an account but is trying to make a new one!"
        elif state == "token_error":
            prompt = f"Generate a brief apology: we couldn't authenticate with SFTPGo while creating '{username}'."
        else:
            prompt = f"Generate a brief message stating that an error occurred while creating the account for '{username}'."

        try:
            resp = await llm_client.chat(messages=[{"role": "user", "content": prompt}])
            text = (resp.get("message", {}) or {}).get("content", "").strip()
        except Exception:
            text = ""

        if state == "created":
            if text:
                text += f"\n\n🔐 Password: `{password}`"
            else:
                text = f"Account created for **{username}**.\n🔐 Password: `{password}`"
        elif not text:
            text = "There was a problem creating your account."

        return text

    async def handle_webui(self, args, llm_client):
        desired = (args or {}).get("username", "").strip()
        if not desired:
            desired = f"guest-{secrets.token_hex(3)}"
        username = self._sanitize_username(desired)

        password = self.generate_random_password()
        state, _welcome = await self.create_sftp_account(username, password, message_obj=None)

        if state == "created":
            return (
                f"✅ Account created for **{username}**.\n"
                f"🔐 Password: `{password}`\n"
                "You can now log in via SFTP."
            )
        if state == "exists":
            return f"ℹ️ The account **{username}** already exists."
        if state == "token_error":
            return "❌ Could not authenticate with SFTPGo. Please check the SFTPGo plugin settings."
        return "❌ Failed to create the SFTP account. Please try again later."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        desired = (args or {}).get("username", "").strip() or user
        username = self._sanitize_username(desired)
        password = self.generate_random_password()

        state, _welcome = await self.create_sftp_account(username, password, message_obj=None)

        if state == "created":
            return f"{user}: Account created for {username}. Password: {password}"
        if state == "exists":
            return f"{user}: The account {username} already exists."
        if state == "token_error":
            return f"{user}: Could not authenticate with SFTPGo. Check settings."
        return f"{user}: Failed to create the SFTP account."

    # -------- Matrix handler --------
    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        """
        Matrix: derive a sane default username from the sender's MXID localpart,
        allow override via args['username'], sanitize, create the account, and
        return a plain text message (the Matrix platform will post it).
        """
        desired = (args or {}).get("username", "").strip()
        if not desired:
            # Use Matrix localpart (e.g., @alice:example.org -> alice)
            desired = self._matrix_localpart(sender)
        username = self._sanitize_username(desired)

        password = self.generate_random_password()
        state, _welcome = await self.create_sftp_account(username, password, message_obj=None)

        if state == "created":
            return (
                f"✅ Account created for **{username}**.\n"
                f"🔐 Password: `{password}`\n"
                "You can now log in via SFTP."
            )
        if state == "exists":
            return f"ℹ️ The account **{username}** already exists."
        if state == "token_error":
            return "❌ Could not authenticate with SFTPGo. Please check the SFTPGo plugin settings."
        return "❌ Failed to create the SFTP account. Please try again later."


plugin = SFTPGoAccountPlugin()
