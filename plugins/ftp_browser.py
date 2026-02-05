# plugins/ftp_browser.py
import discord
import io
import aioftp
from plugin_base import ToolPlugin
from helpers import redis_client

async def safe_send(channel, content: str, **kwargs):
    if len(content) > 2000:
        content = content[:1997] + "..."
    try:
        await channel.send(content, **kwargs)
    except Exception as e:
        print(f"safe_send failed: {e}")

class FtpBrowserPlugin(ToolPlugin):
    name = "ftp_browser"
    plugin_name = "FTP Browser"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "ftp_browser",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Lets the user browse and download files from the FTP server."
    plugin_dec = "Browse and download files from the configured FTP server."
    pretty_name = "Connecting to FTP"
    settings_category = "FTPBrowser"
    required_settings = {
        "FTP_HOST": {
            "label": "FTP Server Host",
            "type": "string",
            "default": "",
            "description": "The hostname or IP of the FTP server."
        },
        "FTP_PORT": {
            "label": "FTP Port",
            "type": "string",
            "default": "21",
            "description": "The port number of the FTP server."
        },
        "FTP_USER": {
            "label": "FTP Username",
            "type": "string",
            "default": "anonymous",
            "description": "Username for FTP login."
        },
        "FTP_PASS": {
            "label": "FTP Password",
            "type": "string",
            "default": "",
            "description": "Password for FTP login."
        }
    }
    waiting_prompt_template = "Write a friendly message telling {mention} you’re loading the FTP browser now! Only output that message."
    platforms = ["discord"]
    max_upload_size_bytes = 25 * 1024 * 1024  # 25 MB

    user_paths = {}

    @staticmethod
    def get_ftp_conn_context():
        settings = redis_client.hgetall("plugin_settings:FTPBrowser")
        host = settings.get("FTP_HOST", "localhost")
        port = int(settings.get("FTP_PORT", 21))
        user = settings.get("FTP_USER", "anonymous")
        passwd = settings.get("FTP_PASS", "")
        return aioftp.Client.context(host, port=port, user=user, password=passwd)

    @staticmethod
    async def list_ftp_files(path="/"):
        async with FtpBrowserPlugin.get_ftp_conn_context() as client:
            await client.change_directory(path)
            entries = []
            async for path_obj, stat in client.list():
                name = path_obj.name
                is_dir = stat['type'] == 'dir'
                entries.append((name, is_dir))
            return entries

    @staticmethod
    async def get_file_size(client, path):
        info = await client.stat(path)
        return int(info.get("size", "0"))

    @staticmethod
    async def download_ftp_file(path):
        async with FtpBrowserPlugin.get_ftp_conn_context() as client:
            stream = io.BytesIO()
            stream_reader = await client.download_stream(path)
            async for block in stream_reader.iter_by_block():
                stream.write(block)
            stream.seek(0)
            return stream

    @staticmethod
    def safe_label(name, is_dir):
        label = f"[DIR] {name}" if is_dir else name
        return label if len(label) <= 80 else label[:77] + "..."

    class FileBrowserView(discord.ui.View):
        def __init__(self, plugin, user_id, current_path, entries, page=0, llm_client=None):
            super().__init__(timeout=300)
            self.plugin = plugin
            self.user_id = user_id
            self.current_path = current_path
            self.page = page
            self.llm_client = llm_client

            start = page * 22
            end = start + 22
            paged_entries = entries[start:end]

            for name, is_dir in paged_entries:
                label = FtpBrowserPlugin.safe_label(("📁 " if is_dir else "📄 ") + name, False)
                self.add_item(FtpBrowserPlugin.FileButton(plugin, label, name, is_dir, user_id, current_path, llm_client))

            if current_path != "/":
                self.add_item(FtpBrowserPlugin.GoBackButton(plugin, user_id, current_path, llm_client))

            if start > 0:
                self.add_item(FtpBrowserPlugin.PageButton("⬅️ Prev", plugin, user_id, current_path, page - 1, llm_client))
            if end < len(entries):
                self.add_item(FtpBrowserPlugin.PageButton("Next ➡️", plugin, user_id, current_path, page + 1, llm_client))

    class FileButton(discord.ui.Button):
        def __init__(self, plugin, label, path, is_dir, user_id, current_path, llm_client):
            super().__init__(label=label, style=discord.ButtonStyle.primary)
            self.plugin = plugin
            self.path = path
            self.is_dir = is_dir
            self.user_id = user_id
            self.current_path = current_path
            self.llm_client = llm_client

        async def callback(self, interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                return await interaction.response.send_message("This is not your session.", ephemeral=True)

            new_path = f"{self.current_path}/{self.path}".replace("//", "/")

            if self.is_dir:
                FtpBrowserPlugin.user_paths[self.user_id] = new_path
                entries = await FtpBrowserPlugin.list_ftp_files(new_path)
                await interaction.response.edit_message(content=f"Browsing: `{new_path}`", view=FtpBrowserPlugin.FileBrowserView(self.plugin, self.user_id, new_path, entries, llm_client=self.llm_client))
            else:
                async with FtpBrowserPlugin.get_ftp_conn_context() as client:
                    size = await FtpBrowserPlugin.get_file_size(client, new_path)

                if size > FtpBrowserPlugin.max_upload_size_bytes:
                    await interaction.response.defer()

                    system_msg = (
                        f"The user tried to download `{self.path}`, but the file is too large "
                        f"to send via Discord (> {size // (1024 * 1024)}MB)."
                    )
                    response = await self.llm_client.chat(
                        messages=[
                            {"role": "system", "content": system_msg},
                            {"role": "user", "content": "Tell them in a friendly way that they should connect to the FTP manually to download this file."}
                        ]
                    )

                    reply = response["message"].get("content", "").strip() or \
                        "That file is a bit too big for Discord. Please use your FTP client to grab it manually!"

                    await interaction.followup.send(reply)

                else:
                    file_data = await FtpBrowserPlugin.download_ftp_file(new_path)
                    await interaction.response.send_message(file=discord.File(fp=file_data, filename=self.path))

    class GoBackButton(discord.ui.Button):
        def __init__(self, plugin, user_id, current_path, llm_client):
            super().__init__(label="⬅️ Go Back", style=discord.ButtonStyle.secondary)
            self.plugin = plugin
            self.user_id = user_id
            self.current_path = current_path
            self.llm_client = llm_client

        async def callback(self, interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                return await interaction.response.send_message("This is not your session.", ephemeral=True)

            new_path = "/".join(self.current_path.rstrip("/").split("/")[:-1]) or "/"
            FtpBrowserPlugin.user_paths[self.user_id] = new_path
            entries = await FtpBrowserPlugin.list_ftp_files(new_path)
            await interaction.response.edit_message(content=f"Browsing: `{new_path}`", view=FtpBrowserPlugin.FileBrowserView(self.plugin, self.user_id, new_path, entries, llm_client=self.llm_client))

    class PageButton(discord.ui.Button):
        def __init__(self, label, plugin, user_id, current_path, page, llm_client):
            super().__init__(label=label, style=discord.ButtonStyle.secondary)
            self.plugin = plugin
            self.user_id = user_id
            self.current_path = current_path
            self.page = page
            self.llm_client = llm_client

        async def callback(self, interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                return await interaction.response.send_message("This is not your session.", ephemeral=True)

            entries = await FtpBrowserPlugin.list_ftp_files(self.current_path)
            await interaction.response.edit_message(content=f"Browsing: `{self.current_path}`", view=FtpBrowserPlugin.FileBrowserView(self.plugin, self.user_id, self.current_path, entries, self.page, llm_client=self.llm_client))

    async def handle_discord(self, message, args, llm_client):
        user_id = message.author.id
        FtpBrowserPlugin.user_paths[user_id] = "/"
        entries = await FtpBrowserPlugin.list_ftp_files("/")

        await safe_send(
            message.channel,
            "Browsing `/`",
            view=FtpBrowserPlugin.FileBrowserView(self, user_id, "/", entries, llm_client=llm_client)
        )

        system_msg = f"The user is now browsing the root directory of an FTP server."
        followup = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": "Send a short friendly message to encourage their FTP browsing. Do not include any instructions — just the message."}
            ]
        )

        message_text = followup["message"].get("content", "").strip() or "Happy browsing!"
        return message_text

    async def handle_webui(self, args, llm_client):
        return "📂 FTP browsing is only available on Discord for now."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        await bot.privmsg(channel, f"{user}: FTP browsing is only available on Discord for now.")

plugin = FtpBrowserPlugin()
