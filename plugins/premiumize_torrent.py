# plugins/premiumize_torrent.py
import os
import aiohttp
import hashlib
import bencodepy
from urllib.parse import quote
import discord
from discord import ui, ButtonStyle
from dotenv import load_dotenv
import logging
import tempfile
import asyncio
from plugin_base import ToolPlugin

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

PREMIUMIZE_API_KEY = os.getenv("PREMIUMIZE_API_KEY")

async def check_premiumize_cache(item: str):
    """
    Check if an item (a URL or torrent hash) is cached on Premiumize.me.
    Returns a tuple: (True, filename) if cached, else (False, None).
    """
    api_url = "https://www.premiumize.me/api/cache/check"
    params = {
        "apikey": PREMIUMIZE_API_KEY,
        "items[]": item
    }
    logger.debug(f"Checking cache for item: {item} with params: {params}")
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, params=params) as response:
            logger.debug(f"Cache check response status: {response.status}")
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Cache check response: {data}")
                if data.get("status") == "success" and data.get("response") and data["response"][0]:
                    filename = data.get("filename", [None])[0] if data.get("filename") else None
                    return True, filename
                else:
                    return False, None
            else:
                logger.error(f"Cache check failed: {response.status}")
                return False, None

def extract_torrent_hash(file_path: str) -> str:
    """
    Extract the torrent hash (SHA-1 in uppercase hex) from a torrent file.
    """
    try:
        with open(file_path, "rb") as f:
            torrent_data = f.read()
        decoded_data = bencodepy.decode(torrent_data)
        info_dict = decoded_data[b'info']
        encoded_info = bencodepy.encode(info_dict)
        torrent_hash = hashlib.sha1(encoded_info).hexdigest().upper()
        return torrent_hash
    except Exception as e:
        logger.error(f"Failed to extract torrent hash: {e}")
        return None

def create_magnet_link(torrent_hash: str) -> str:
    return f"magnet:?xt=urn:btih:{torrent_hash}"

def encode_filename(filename: str) -> str:
    from urllib.parse import quote
    return quote(filename)

async def get_premiumize_download_links(item: str):
    """
    Fetch download links for an item (URL or magnet link) from Premiumize.me.
    """
    api_url = "https://www.premiumize.me/api/transfer/directdl"
    payload = {
        "apikey": PREMIUMIZE_API_KEY,
        "src": item
    }
    logger.debug(f"Fetching download links for item: {item} with payload: {payload}")
    async with aiohttp.ClientSession() as session:
        async with session.post(api_url, data=payload) as response:
            logger.debug(f"Download links response status: {response.status}")
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Download links response: {data}")
                if data.get("status") == "success":
                    links = data.get("content", [])
                    return links
                else:
                    logger.error(f"Error in download links: {data.get('message')}")
                    return None
            else:
                logger.error(f"Failed to connect to Premiumize.me: {response.status}")
                return None

async def process_torrent(channel: discord.TextChannel, torrent_attachment: discord.Attachment, max_response_length=2000):
    """
    Process a Premiumize torrent request for Discord.
    Saves the torrent file, extracts its hash, checks cache status,
    fetches download links, and sends the results.
    """
    logger.debug(f"Processing torrent for file: {torrent_attachment.filename}")
    file_path = f"./{torrent_attachment.filename}"
    await torrent_attachment.save(file_path)
    try:
        torrent_hash = extract_torrent_hash(file_path)
        if not torrent_hash:
            await channel.send(content="Failed to extract torrent hash from the file.")
            return ""
        logger.debug(f"Torrent hash: {torrent_hash}")
        cached, filename = await check_premiumize_cache(torrent_hash)
        logger.debug(f"Cache check: cached={cached}, filename={filename}")
        if cached:
            magnet_link = create_magnet_link(torrent_hash)
            download_links = await get_premiumize_download_links(magnet_link)
            if download_links:
                if len(download_links) > 10:
                    view = PaginatedLinks(download_links, f"Download Links for `{filename}`")
                    await channel.send(content=view.get_page_content(), view=view)
                else:
                    links_message = f"**Download Links for `{filename}`:**\n"
                    for file in download_links:
                        encoded_filename = encode_filename(file['path'])
                        encoded_link = file['link'].replace(file['path'], encoded_filename)
                        new_line = f"- [{file['path']}]({encoded_link})\n"
                        if len(links_message) + len(new_line) > max_response_length:
                            break
                        links_message += new_line
                    await channel.send(content=links_message)
            else:
                await channel.send(content="Failed to fetch download links.")
        else:
            await channel.send(content=f"The torrent `{torrent_attachment.filename}` is not cached on Premiumize.me.")
    except Exception as e:
        logger.error(f"Error processing torrent: {e}")
        await channel.send(content="An error occurred while processing the torrent file.")
    finally:
        import os
        if os.path.exists(file_path):
            os.remove(file_path)
    return ""

# A simple paginated view for Discord.
class PaginatedLinks(ui.View):
    def __init__(self, links, title, page_size=10):
        super().__init__()
        self.links = links
        self.title = title
        self.page_size = page_size
        self.current_page = 0
        self.update_buttons()
    def get_page_content(self):
        start = self.current_page * self.page_size
        end = start + self.page_size
        page_links = self.links[start:end]
        links_message = f"**{self.title} (Page {self.current_page + 1}):**\n"
        for file in page_links:
            encoded_filename = encode_filename(file['path'])
            encoded_link = file['link'].replace(file['path'], encoded_filename)
            new_line = f"- [{file['path']}]({encoded_link})\n"
            if len(links_message) + len(new_line) > 2000:
                break
            links_message += new_line
        return links_message
    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = (self.current_page + 1) * self.page_size >= len(self.links)
    @ui.button(label="Previous", style=ButtonStyle.grey)
    async def previous_button(self, interaction: discord.Interaction, button: ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(content=self.get_page_content(), view=self)
    @ui.button(label="Next", style=ButtonStyle.grey)
    async def next_button(self, interaction: discord.Interaction, button: ui.Button):
        if (self.current_page + 1) * self.page_size < len(self.links):
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(content=self.get_page_content(), view=self)

class PremiumizeTorrentPlugin(ToolPlugin):
    name = "premiumize_torrent"
    usage = (
        "{\n"
        '  "function": "premiumize_torrent",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Checks if a torrent file provided by the user is cached on premiumize.me."
    platforms = ["discord"]

    async def handle_webui(self, args, ollama_client, context_length):
        # Web UI does not support torrent processing.
        return "For premiumize torrent use attachment on the left to attach a .torrent file."

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        if message.attachments:
            torrent_attachment = message.attachments[0]
            waiting_prompt = (
                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I check Premiumize for that torrent and retrieve download links for them. Only generate the message. Do not respond to this message."
            )
            waiting_response = await ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "system", "content": waiting_prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": context_length}
            )
            waiting_text = waiting_response['message'].get('content', '')
            if waiting_text:
                await message.channel.send(waiting_text)
            else:
                await message.channel.send("Hold on while I check Premiumize for that torrent...")
            async with message.channel.typing():
                result = await process_torrent(message.channel, torrent_attachment, max_response_length)
                return result
        else:
            prompt = f"Generate an error message to {message.author.mention} explaining that no torrent file was attached for Premiumize torrent check. Only generate the message. Do not respond to this message."
            error_msg = await self.generate_error_message(prompt, "No torrent file attached for Premiumize torrent check.", message)
            return error_msg

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

# Export an instance of the plugin.
plugin = PremiumizeTorrentPlugin()