import io
import logging
import posixpath
from typing import Any, Dict, List, Tuple

import aioftp
import discord

from helpers import redis_client
from plugin_base import ToolPlugin

logger = logging.getLogger("ftp_browser")
logger.setLevel(logging.INFO)


async def safe_send(channel, content: str, **kwargs):
    if len(content) > 2000:
        content = content[:1997] + "..."
    try:
        await channel.send(content, **kwargs)
    except Exception:
        logger.exception("safe_send failed")


class FtpBrowserPlugin(ToolPlugin):
    name = "ftp_browser"
    plugin_name = "FTP Browser"
    version = "1.1.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "ftp_browser",\n'
        '  "arguments": {"path": "/optional/path", "page": 0}\n'
        "}\n"
    )
    optional_args = ["path", "page"]
    description = "Lets the user browse and download files from the FTP server."
    plugin_dec = "Browse and download files from the configured FTP server."
    pretty_name = "Connecting to FTP"
    settings_category = "FTPBrowser"
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are loading the FTP browser now. "
        "Only output that message."
    )
    platforms = ["discord"]
    required_settings = {
        "FTP_HOST": {
            "label": "FTP Server Host",
            "type": "string",
            "default": "",
            "description": "The hostname or IP of the FTP server.",
        },
        "FTP_PORT": {
            "label": "FTP Port",
            "type": "number",
            "default": 21,
            "description": "The port number of the FTP server.",
        },
        "FTP_USER": {
            "label": "FTP Username",
            "type": "string",
            "default": "anonymous",
            "description": "Username for FTP login.",
        },
        "FTP_PASS": {
            "label": "FTP Password",
            "type": "password",
            "default": "",
            "description": "Password for FTP login.",
        },
    }

    max_upload_size_bytes = 25 * 1024 * 1024  # 25 MB
    items_per_page = 22
    user_paths: Dict[int, str] = {}

    @staticmethod
    def _decode_map(raw: Dict[Any, Any] | None) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            elif value is None:
                out[k] = ""
            else:
                out[k] = str(value)
        return out

    @staticmethod
    def _coerce_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(str(value).strip()))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    @classmethod
    def _normalize_path(cls, raw_path: Any) -> str:
        path = str(raw_path or "/").strip().replace("\\", "/")
        if not path:
            path = "/"
        if not path.startswith("/"):
            path = f"/{path}"
        normalized = posixpath.normpath(path)
        if normalized in {"", "."}:
            return "/"
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        return normalized

    @classmethod
    def _join_path(cls, current_path: str, item_name: str) -> str:
        safe_name = str(item_name or "").replace("\\", "/").strip()
        if "/" in safe_name:
            safe_name = posixpath.basename(safe_name.rstrip("/"))
        joined = posixpath.join(cls._normalize_path(current_path), safe_name)
        return cls._normalize_path(joined)

    @classmethod
    def _load_settings(cls) -> Dict[str, str]:
        keys = [
            "plugin_settings: FTP Browser",
            "plugin_settings:FTP Browser",
            "plugin_settings: FTPBrowser",
            "plugin_settings:FTPBrowser",
            f"plugin_settings: {cls.settings_category}",
            f"plugin_settings:{cls.settings_category}",
        ]
        merged: Dict[str, str] = {}
        for key in keys:
            merged.update(cls._decode_map(redis_client.hgetall(key) or {}))
        return merged

    @classmethod
    def get_ftp_conn_context(cls):
        settings = cls._load_settings()
        host = (settings.get("FTP_HOST") or "").strip()
        if not host:
            raise ValueError("FTP host is not configured. Set FTP_HOST in the FTP Browser settings.")
        port = cls._coerce_int(settings.get("FTP_PORT", 21), default=21, minimum=1, maximum=65535)
        user = (settings.get("FTP_USER") or "anonymous").strip() or "anonymous"
        passwd = settings.get("FTP_PASS", "") or ""
        return aioftp.Client.context(
            host=host,
            port=port,
            user=user,
            password=passwd,
            socket_timeout=10,
            connection_timeout=10,
            path_timeout=10,
        )

    @classmethod
    async def list_ftp_files(cls, path: str = "/") -> List[Tuple[str, bool]]:
        safe_path = cls._normalize_path(path)
        async with cls.get_ftp_conn_context() as client:
            await client.change_directory(safe_path)
            entries: List[Tuple[str, bool]] = []
            async for path_obj, stat in client.list():
                name = getattr(path_obj, "name", "") or str(path_obj)
                if name in {"", ".", ".."}:
                    continue
                is_dir = str((stat or {}).get("type", "")).lower() == "dir"
                entries.append((name, is_dir))
        entries.sort(key=lambda item: (not item[1], item[0].lower()))
        return entries

    @classmethod
    async def get_file_size(cls, client, path: str) -> int:
        info = await client.stat(path)
        return cls._coerce_int((info or {}).get("size", "0"), default=0, minimum=0, maximum=1024**5)

    @classmethod
    async def download_ftp_file(cls, path: str):
        async with cls.get_ftp_conn_context() as client:
            stream = io.BytesIO()
            stream_reader = await client.download_stream(path)
            async for block in stream_reader.iter_by_block():
                stream.write(block)
            stream.seek(0)
            return stream

    @staticmethod
    def safe_label(name: str, is_dir: bool) -> str:
        label = f"[DIR] {name}" if is_dir else name
        return label if len(label) <= 80 else f"{label[:77]}..."

    @classmethod
    def _clamp_page(cls, page: int, entry_count: int) -> int:
        if entry_count <= 0:
            return 0
        max_page = (entry_count - 1) // cls.items_per_page
        return min(max(page, 0), max_page)

    @staticmethod
    async def _send_interaction_error(interaction: discord.Interaction, text: str):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(text, ephemeral=True)
            else:
                await interaction.response.send_message(text, ephemeral=True)
        except Exception:
            logger.exception("Failed to send interaction error")

    @staticmethod
    async def _large_file_reply(file_name: str, size_bytes: int, llm_client) -> str:
        size_mb = max(1, size_bytes // (1024 * 1024))
        fallback = (
            f"`{file_name}` is about {size_mb} MB, which is too large for Discord uploads. "
            "Please download it directly with your FTP client."
        )
        if llm_client is None:
            return fallback
        try:
            response = await llm_client.chat(
                messages=[
                    {
                        "role": "system",
                        "content": f"User tried to download `{file_name}` but it is too large ({size_mb} MB).",
                    },
                    {
                        "role": "user",
                        "content": "Write one short friendly sentence asking them to download it directly via FTP.",
                    },
                ]
            )
            text = ((response or {}).get("message", {}) or {}).get("content", "").strip()
            return text or fallback
        except Exception:
            logger.exception("Failed generating large-file response")
            return fallback

    class FileBrowserView(discord.ui.View):
        def __init__(self, plugin, user_id, current_path, entries, page=0, llm_client=None):
            super().__init__(timeout=300)
            self.plugin = plugin
            self.user_id = user_id
            self.current_path = current_path
            self.llm_client = llm_client
            self.page = FtpBrowserPlugin._clamp_page(page, len(entries))

            start = self.page * FtpBrowserPlugin.items_per_page
            end = start + FtpBrowserPlugin.items_per_page
            paged_entries = entries[start:end]

            for name, is_dir in paged_entries:
                icon_name = f"{'📁' if is_dir else '📄'} {name}"
                label = FtpBrowserPlugin.safe_label(icon_name, False)
                self.add_item(
                    FtpBrowserPlugin.FileButton(
                        plugin,
                        label,
                        name,
                        is_dir,
                        user_id,
                        current_path,
                        llm_client,
                    )
                )

            if current_path != "/":
                self.add_item(FtpBrowserPlugin.GoBackButton(plugin, user_id, current_path, llm_client))

            if start > 0:
                self.add_item(
                    FtpBrowserPlugin.PageButton(
                        "⬅️ Prev",
                        plugin,
                        user_id,
                        current_path,
                        self.page - 1,
                        llm_client,
                    )
                )
            if end < len(entries):
                self.add_item(
                    FtpBrowserPlugin.PageButton(
                        "Next ➡️",
                        plugin,
                        user_id,
                        current_path,
                        self.page + 1,
                        llm_client,
                    )
                )

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

            new_path = FtpBrowserPlugin._join_path(self.current_path, self.path)
            try:
                if self.is_dir:
                    FtpBrowserPlugin.user_paths[self.user_id] = new_path
                    entries = await FtpBrowserPlugin.list_ftp_files(new_path)
                    return await interaction.response.edit_message(
                        content=f"Browsing: `{new_path}`",
                        view=FtpBrowserPlugin.FileBrowserView(
                            self.plugin,
                            self.user_id,
                            new_path,
                            entries,
                            llm_client=self.llm_client,
                        ),
                    )

                async with FtpBrowserPlugin.get_ftp_conn_context() as client:
                    size = await FtpBrowserPlugin.get_file_size(client, new_path)

                if size > FtpBrowserPlugin.max_upload_size_bytes:
                    await interaction.response.defer()
                    reply = await FtpBrowserPlugin._large_file_reply(self.path, size, self.llm_client)
                    await interaction.followup.send(reply)
                    return

                file_data = await FtpBrowserPlugin.download_ftp_file(new_path)
                await interaction.response.send_message(file=discord.File(fp=file_data, filename=self.path))
            except Exception as exc:
                logger.exception("FTP browser button failed for path %s", new_path)
                await FtpBrowserPlugin._send_interaction_error(interaction, f"❌ Could not open `{new_path}`: {exc}")

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

            try:
                parent = posixpath.dirname(self.current_path.rstrip("/")) or "/"
                new_path = FtpBrowserPlugin._normalize_path(parent)
                FtpBrowserPlugin.user_paths[self.user_id] = new_path
                entries = await FtpBrowserPlugin.list_ftp_files(new_path)
                await interaction.response.edit_message(
                    content=f"Browsing: `{new_path}`",
                    view=FtpBrowserPlugin.FileBrowserView(
                        self.plugin,
                        self.user_id,
                        new_path,
                        entries,
                        llm_client=self.llm_client,
                    ),
                )
            except Exception as exc:
                logger.exception("FTP browser go-back failed for user %s", self.user_id)
                await FtpBrowserPlugin._send_interaction_error(interaction, f"❌ Could not go back: {exc}")

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

            try:
                entries = await FtpBrowserPlugin.list_ftp_files(self.current_path)
                page = FtpBrowserPlugin._clamp_page(self.page, len(entries))
                await interaction.response.edit_message(
                    content=f"Browsing: `{self.current_path}`",
                    view=FtpBrowserPlugin.FileBrowserView(
                        self.plugin,
                        self.user_id,
                        self.current_path,
                        entries,
                        page,
                        llm_client=self.llm_client,
                    ),
                )
            except Exception as exc:
                logger.exception("FTP browser page switch failed for path %s", self.current_path)
                await FtpBrowserPlugin._send_interaction_error(interaction, f"❌ Could not change pages: {exc}")

    async def handle_discord(self, message, args, llm_client):
        user_id = message.author.id
        path = self._normalize_path((args or {}).get("path", "/"))
        page = self._coerce_int((args or {}).get("page", 0), default=0, minimum=0, maximum=10000)
        FtpBrowserPlugin.user_paths[user_id] = path

        try:
            entries = await FtpBrowserPlugin.list_ftp_files(path)
        except Exception as exc:
            await safe_send(message.channel, f"❌ Failed to access `{path}`: {exc}")
            return "There was a problem accessing that FTP folder."

        if not entries:
            await safe_send(message.channel, f"📁 `{path}` is empty.")
            return f"`{path}` is empty."

        page = FtpBrowserPlugin._clamp_page(page, len(entries))
        await safe_send(
            message.channel,
            f"📁 Browsing `{path}`",
            view=FtpBrowserPlugin.FileBrowserView(self, user_id, path, entries, page=page, llm_client=llm_client),
        )

        if llm_client is None:
            return "FTP browser is ready."

        try:
            followup = await llm_client.chat(
                messages=[
                    {
                        "role": "system",
                        "content": f"The user is browsing FTP path `{path}` and there are {len(entries)} entries.",
                    },
                    {
                        "role": "user",
                        "content": "Write one short friendly message about the browser being ready. No instructions.",
                    },
                ]
            )
            return ((followup or {}).get("message", {}) or {}).get("content", "").strip() or "FTP browser is ready."
        except Exception:
            logger.exception("Failed to generate FTP browser follow-up message")
            return "FTP browser is ready."

    async def handle_webui(self, args, llm_client):
        return "📂 FTP browsing is only available on Discord for now."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        await bot.privmsg(channel, f"{user}: FTP browsing is only available on Discord for now.")


plugin = FtpBrowserPlugin()
