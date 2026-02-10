import discord
import io
import aiohttp
from plugin_base import ToolPlugin
from helpers import redis_client

async def safe_send(channel, content: str, **kwargs):
    if len(content) > 2000:
        content = content[:1997] + "..."
    try:
        await channel.send(content, **kwargs)
    except Exception as e:
        print(f"safe_send failed: {e}")

class WebDAVBrowserPlugin(ToolPlugin):
    name = "webdav_browser"
    plugin_name = "WebDAV Browser"
    version = "1.0.0"
    min_tater_version = "50"
    usage = '{"function":"webdav_browser","arguments":{"path":"/optional/path"}}'
    description = "Lets the user browse and download files from the WebDAV server."
    plugin_dec = "Browse and download files from the configured WebDAV server."
    pretty_name = "Connecting to WebDAV"
    settings_category = "WebDAV Browser"
    platforms = ["discord"]
    waiting_prompt_template = "Write a friendly message telling {mention} you're exploring the WebDAV directory. Only output that message."

    required_settings = {
        "WEBDAV_URL": {
            "label": "WebDAV Base URL",
            "type": "string",
            "default": "https://example.com/dav",
            "description": "Base URL to your WebDAV directory (no trailing slash)."
        },
        "WEBDAV_USER": {
            "label": "WebDAV Username",
            "type": "string",
            "default": "",
            "description": "Username for WebDAV login."
        },
        "WEBDAV_PASS": {
            "label": "WebDAV Password",
            "type": "password",
            "default": "",
            "description": "Password for WebDAV login."
        }
    }

    max_upload_size_bytes = 25 * 1024 * 1024  # 25 MB
    user_paths = {}

    @staticmethod
    def get_settings():
        settings = redis_client.hgetall("plugin_settings:WebDAVBrowser")
        return {
            "url": settings.get("WEBDAV_URL", "").rstrip("/"),
            "auth": aiohttp.BasicAuth(settings.get("WEBDAV_USER", ""), settings.get("WEBDAV_PASS", ""))
        }

    @staticmethod
    async def list_webdav_files(path="/"):
        config = WebDAVBrowserPlugin.get_settings()
        url = f"{config['url'].rstrip('/')}{path}"

        async with aiohttp.ClientSession(auth=config["auth"]) as session:
            async with session.request("PROPFIND", url, headers={"Depth": "1"}) as resp:
                text = await resp.text()

                if resp.status >= 400:
                    return []

                try:
                    from xml.etree import ElementTree as ET
                    ns = {"d": "DAV:"}
                    root = ET.fromstring(text)
                    entries = []
                    base_href = path.rstrip("/") or "/"

                    for response in root.findall("d:response", ns):
                        href = response.find("d:href", ns).text
                        if href.strip("/") == base_href.strip("/"):
                            continue
                        propstat = response.find("d:propstat", ns)
                        res_type = propstat.find(".//d:resourcetype", ns)
                        is_dir = res_type.find("d:collection", ns) is not None
                        name = href.rstrip("/").split("/")[-1]
                        entries.append((name, is_dir))
                    return sorted(entries)
                except Exception:
                    return []

    @staticmethod
    async def get_file_size(path):
        config = WebDAVBrowserPlugin.get_settings()
        url = f"{config['url']}{path}"
        async with aiohttp.ClientSession(auth=config["auth"]) as session:
            async with session.head(url) as resp:
                return int(resp.headers.get("Content-Length", "0"))

    @staticmethod
    async def download_webdav_file(path):
        config = WebDAVBrowserPlugin.get_settings()
        url = f"{config['url']}{path}"
        async with aiohttp.ClientSession(auth=config["auth"]) as session:
            async with session.get(url) as resp:
                data = await resp.read()
                return io.BytesIO(data)

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
                label = WebDAVBrowserPlugin.safe_label(("📁 " if is_dir else "📄 ") + name, False)
                self.add_item(WebDAVBrowserPlugin.FileButton(plugin, label, name, is_dir, user_id, current_path, llm_client))

            if current_path != "/":
                self.add_item(WebDAVBrowserPlugin.GoBackButton(plugin, user_id, current_path, llm_client))

            if start > 0:
                self.add_item(WebDAVBrowserPlugin.PageButton("⬅️ Prev", plugin, user_id, current_path, page - 1, llm_client))
            if end < len(entries):
                self.add_item(WebDAVBrowserPlugin.PageButton("Next ➡️", plugin, user_id, current_path, page + 1, llm_client))

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
                WebDAVBrowserPlugin.user_paths[self.user_id] = new_path
                entries = await WebDAVBrowserPlugin.list_webdav_files(new_path)
                await interaction.response.edit_message(content=f"Browsing: `{new_path}`", view=WebDAVBrowserPlugin.FileBrowserView(self.plugin, self.user_id, new_path, entries, llm_client=self.llm_client))
            else:
                size = await WebDAVBrowserPlugin.get_file_size(new_path)
                if size > WebDAVBrowserPlugin.max_upload_size_bytes:
                    await interaction.response.defer()
                    response = await self.llm_client.chat(
                        messages=[
                            {"role": "system", "content": f"User tried to download `{self.path}` which is too large."},
                            {"role": "user", "content": "Tell them to download it manually via WebDAV."}
                        ]
                    )
                    reply = response["message"].get("content", "").strip() or "That file is too big to send here — try downloading it directly using a WebDAV client."
                    await interaction.followup.send(reply)
                else:
                    file_data = await WebDAVBrowserPlugin.download_webdav_file(new_path)
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
            WebDAVBrowserPlugin.user_paths[self.user_id] = new_path
            entries = await WebDAVBrowserPlugin.list_webdav_files(new_path)
            await interaction.response.edit_message(content=f"Browsing: `{new_path}`", view=WebDAVBrowserPlugin.FileBrowserView(self.plugin, self.user_id, new_path, entries, llm_client=self.llm_client))

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

            entries = await WebDAVBrowserPlugin.list_webdav_files(self.current_path)
            await interaction.response.edit_message(content=f"Browsing: `{self.current_path}`", view=WebDAVBrowserPlugin.FileBrowserView(self.plugin, self.user_id, self.current_path, entries, self.page, llm_client=self.llm_client))

    async def handle_discord(self, message, args, llm_client):
        user_id = message.author.id
        path = args.get("path", "/")
        page = int(args.get("page", 0))

        WebDAVBrowserPlugin.user_paths[user_id] = path

        try:
            entries = await WebDAVBrowserPlugin.list_webdav_files(path)
        except Exception as e:
            await safe_send(message.channel, f"❌ Failed to access `{path}`: {e}")
            return "There was a problem accessing that folder."

        if not entries:
            await safe_send(message.channel, f"📁 `{path}` is empty.")
            return f"`{path}` is empty — maybe try another folder?"

        await safe_send(
            message.channel,
            f"📁 Browsing `{path}`",
            view=WebDAVBrowserPlugin.FileBrowserView(self, user_id, path, entries, page=page, llm_client=llm_client)
        )

        system_msg = f"The user is now browsing the WebDAV server path: `{path}` with {len(entries)} entries."
        followup = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": "Send a short friendly message to encourage their WebDAV browsing. Do not include any instructions — just the message."}
            ]
        )

        return followup["message"].get("content", "").strip() or "Happy browsing!"

    async def handle_webui(self, args, llm_client):
        return "📂 WebDAV browsing is only available on Discord for now."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        await bot.privmsg(channel, f"{user}: WebDAV browsing is only available on Discord for now.")

plugin = WebDAVBrowserPlugin()
