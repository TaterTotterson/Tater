# premiumize.py

import os
import aiohttp
import hashlib
import bencodepy
from urllib.parse import quote
import discord
from discord import ButtonStyle, ui
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logger for this module.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Premiumize API key from .env
PREMIUMIZE_API_KEY = os.getenv('PREMIUMIZE_API_KEY')

#############################
# Helper Functions & Views
#############################

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
    logger.debug(f"Checking cache for item: {item} using API URL: {api_url} with params: {params}")
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, params=params) as response:
            logger.debug(f"Cache check response status: {response.status}")
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Cache Check Response Data: {data}")
                if data.get("status") == "success" and data.get("response") and data["response"][0]:
                    filename = data.get("filename", [None])[0] if data.get("filename") else None
                    logger.debug(f"Item is cached. Filename: {filename}")
                    return True, filename
                else:
                    logger.debug("Item not found in cache.")
                    return False, None
            else:
                logger.error(f"Failed to check cache with Premiumize.me: {response.status}")
                return False, None

async def get_premiumize_download_links(item: str):
    """
    Fetch download links for all files associated with an item (a URL or magnet link).
    Returns a list of file dictionaries if successful; otherwise, returns None.
    """
    api_url = "https://www.premiumize.me/api/transfer/directdl"
    payload = {
        "apikey": PREMIUMIZE_API_KEY,
        "src": item
    }
    logger.debug(f"Fetching download links for item: {item} using API URL: {api_url} with payload: {payload}")
    async with aiohttp.ClientSession() as session:
        async with session.post(api_url, data=payload) as response:
            logger.debug(f"Download links response status: {response.status}")
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Download Links Response Data: {data}")
                if data.get("status") == "success":
                    links = data.get("content", [])
                    logger.debug(f"Retrieved {len(links)} download links.")
                    return links
                else:
                    logger.error(f"Failed to fetch download links: {data.get('message')}")
                    return None
            else:
                logger.error(f"Failed to connect to Premiumize.me: {response.status}")
                return None

def extract_torrent_hash(file_path: str) -> str:
    """
    Extract the torrent hash from a torrent file.
    Returns the SHA-1 hash (in uppercase hex) of the torrent's info dictionary.
    """
    logger.debug(f"Extracting torrent hash from file: {file_path}")
    try:
        with open(file_path, "rb") as f:
            torrent_data = f.read()
        decoded_data = bencodepy.decode(torrent_data)
        info_dict = decoded_data[b'info']
        encoded_info = bencodepy.encode(info_dict)
        torrent_hash = hashlib.sha1(encoded_info).hexdigest().upper()
        logger.debug(f"Extracted torrent hash: {torrent_hash}")
        return torrent_hash
    except Exception as e:
        logger.error(f"Failed to extract torrent hash: {e}")
        return None

def create_magnet_link(torrent_hash: str) -> str:
    """
    Convert a torrent hash into a magnet link.
    """
    magnet_link = f"magnet:?xt=urn:btih:{torrent_hash}"
    logger.debug(f"Created magnet link: {magnet_link}")
    return magnet_link

def encode_filename(filename: str) -> str:
    """
    URL-encode a filename to handle spaces and special characters.
    """
    encoded = quote(filename)
    logger.debug(f"Encoded filename: {filename} to {encoded}")
    return encoded

class PaginatedLinks(ui.View):
    """
    A pagination view to display download links in Discord.
    """
    def __init__(self, links, title, page_size=10):
        super().__init__()
        self.links = links
        self.title = title
        self.page_size = page_size
        self.current_page = 0
        self.update_buttons()
        logger.debug(f"PaginatedLinks initialized with {len(links)} links, title: {title}, page_size: {page_size}")

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
        logger.debug(f"Generated page content: {links_message}")
        return links_message

    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = (self.current_page + 1) * self.page_size >= len(self.links)
        logger.debug(f"Updated buttons: current_page: {self.current_page}, total links: {len(self.links)}")

    @ui.button(label="Previous", style=ButtonStyle.grey)
    async def previous_button(self, interaction: discord.Interaction, button: ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            logger.debug(f"Previous button clicked. New current_page: {self.current_page}")
            await interaction.response.edit_message(content=self.get_page_content(), view=self)

    @ui.button(label="Next", style=ButtonStyle.grey)
    async def next_button(self, interaction: discord.Interaction, button: ui.Button):
        if (self.current_page + 1) * self.page_size < len(self.links):
            self.current_page += 1
            self.update_buttons()
            logger.debug(f"Next button clicked. New current_page: {self.current_page}")
            await interaction.response.edit_message(content=self.get_page_content(), view=self)

#############################
# Command Processing Functions (Updated to use a Channel)
#############################

async def process_download(channel: discord.TextChannel, url: str, max_response_length=2000):
    """
    Process a Premiumize download request for a URL.
    Fetches download links and sends them back via the provided channel.
    """
    logger.debug(f"Processing Premiumize download for URL: {url}")
    download_links = await get_premiumize_download_links(url)
    if download_links:
        logger.debug(f"Found {len(download_links)} download links for URL: {url}")
        if len(download_links) > 10:
            view = PaginatedLinks(download_links, f"Download Links for `{url}`")
            await channel.send(content=view.get_page_content(), view=view)
        else:
            links_message = f"**Download Links for `{url}`:**\n"
            for file in download_links:
                encoded_filename = encode_filename(file['path'])
                encoded_link = file['link'].replace(file['path'], encoded_filename)
                new_line = f"- [{file['path']}]({encoded_link})\n"
                if len(links_message) + len(new_line) > max_response_length:
                    break
                links_message += new_line
            logger.debug(f"Sending download links message: {links_message}")
            await channel.send(content=links_message)
    else:
        logger.debug(f"No download links found for URL: {url}")
        await channel.send(content=f"The URL `{url}` is not cached on Premiumize.me.")

async def process_torrent(channel: discord.TextChannel, torrent_attachment: discord.Attachment, max_response_length=2000):
    """
    Process a Premiumize torrent request.
    Saves the attached torrent file temporarily, extracts its hash, checks cache status,
    fetches download links if cached, sends the results via the provided channel,
    and finally deletes the torrent file.
    """
    logger.debug(f"Processing Premiumize torrent for file: {torrent_attachment.filename}")
    file_path = f"./{torrent_attachment.filename}"
    await torrent_attachment.save(file_path)
    logger.debug(f"Saved torrent file to {file_path}")
    
    try:
        torrent_hash = extract_torrent_hash(file_path)
        if not torrent_hash:
            logger.error("Failed to extract torrent hash.")
            await channel.send(content="Failed to extract torrent hash from the file.")
            return

        logger.debug(f"Torrent hash: {torrent_hash}")
        cached, filename = await check_premiumize_cache(torrent_hash)
        logger.debug(f"Cache check for torrent hash returned: cached={cached}, filename={filename}")
        if cached:
            magnet_link = create_magnet_link(torrent_hash)
            logger.debug(f"Magnet link: {magnet_link}")
            download_links = await get_premiumize_download_links(magnet_link)
            if download_links:
                logger.debug(f"Found {len(download_links)} download links for torrent.")
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
                    logger.debug(f"Sending download links message for torrent: {links_message}")
                    await channel.send(content=links_message)
            else:
                logger.error("Failed to fetch download links for torrent.")
                await channel.send(content="Failed to fetch download links.")
        else:
            logger.debug(f"Torrent file {torrent_attachment.filename} is not cached.")
            await channel.send(content=f"The torrent `{torrent_attachment.filename}` is not cached.")
    except Exception as e:
        logger.error(f"Error processing torrent: {e}")
        await channel.send(content="An error occurred while processing the torrent file.")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Deleted torrent file {file_path} from disk.")