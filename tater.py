# tater.py
import os
import json
import asyncio
import logging
import redis
import discord
from discord.ext import commands
import ollama
from dotenv import load_dotenv
import re
import YouTube  # Module for YouTube summarization functions
import web      # Module for webpage summarization functions
import premiumize  # Module for Premiumize-related functions

# Load environment variables
load_dotenv()
ollama_model = os.getenv('OLLAMA_MODEL', 'llama3.2').strip()
response_channel_id = int(os.getenv("RESPONSE_CHANNEL_ID", 0))
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
max_response_length = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))
ollama_temperature = float(os.getenv('OLLAMA_TEMPERATURE', 0.6))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('discord.tater')

# Initialize Redis client
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

def clear_redis():
    """Clear all keys in Redis."""
    try:
        redis_client.flushdb()
        logger.info("Where am I?!? What happened?!?")
    except Exception as e:
        logger.error(f"Error clearing Redis: {e}")
        raise

class tater(commands.Bot):
    def __init__(self, ollama_client, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ollama = ollama_client
        self.model = ollama_model

    async def setup_hook(self):
        await setup_commands(self)

    async def on_ready(self):
        activity = discord.Activity(name='tater', state='Ask me anything!', type=discord.ActivityType.custom)
        await self.change_presence(activity=activity)
        logger.info('Bot is ready and active.')

    async def generate_error_message(self, prompt: str, fallback: str, message: discord.Message):
        """
        Uses Ollama to generate a friendly error message based on a prompt.
        Returns the generated message or the fallback text if generation fails.
        """
        try:
            error_response = await self.ollama.chat(
                model=self.model,
                messages=[{"role": "system", "content": prompt}],
                stream=False,
                keep_alive=-1
            )
            error_text = error_response['message'].get('content', '').strip()
            if error_text:
                return error_text
        except Exception as e:
            logger.error(f"Error generating error message: {e}")
        return fallback

    async def on_message(self, message: discord.Message):
        # Ignore messages from the bot itself.
        if message.author == self.user:
            return

        # Process only if the message is in the designated channel or if the bot is mentioned.
        should_respond = message.channel.id == response_channel_id or self.user.mentioned_in(message)
        if not should_respond:
            return

        logger.debug(f"Received message: {message.content} from {message.author}")

        # Build a system prompt explaining the available function calls.
        system_prompt = (
            "You are Tater Totterson, A retro gaming enthusiast who is part of the DNServ Crew"
            "The DNServ Crew is an Elite, tight-knit, Retro Gaming Group. You help the DNServ Crew with tools"
            "You have access to five tools:\n"
            "1. 'youtube_summary' for summarizing YouTube videos, pretend you have to watch the whole video to get a summary,\n"
            "2. 'web_summary' for summarizing news articles or text from any webpage, pretend you have to watch the read the whole article to get a summary,\n"
            "3. 'draw_picture' for generating images, pretend you are actually drawing the picture yourself,\n"
            "4. 'premiumize_download' for checking if a URL is cached on Premiumize.me and retrieving download links, and\n"
            "5. 'premiumize_torrent' for checking if a torrent file is cached on Premiumize.me and retrieving download links.\n\n"
            "When a user asks for a summary, image generation, or Premiumize file check, reply ONLY with a JSON object in one "
            "of the following formats (and nothing else):\n\n"
            "For YouTube videos:\n"
            "{\n"
            '  "function": "youtube_summary",\n'
            '  "arguments": {\n'
            '      "video_url": "<YouTube URL>"\n'
            "  }\n"
            "}\n\n"
            "For webpages:\n"
            "{\n"
            '  "function": "web_summary",\n'
            '  "arguments": {\n'
            '      "url": "<Webpage URL>"\n'
            "  }\n"
            "}\n\n"
            "For drawing images:\n"
            "{\n"
            '  "function": "draw_picture",\n'
            '  "arguments": {\n'
            '      "prompt": "<Text prompt for the image>"\n'
            "  }\n"
            "}\n\n"
            "For Premiumize URL download check:\n"
            "{\n"
            '  "function": "premiumize_download",\n'
            '  "arguments": {\n'
            '      "url": "<URL to check on Premiumize.me>"\n'
            "  }\n"
            "}\n\n"
            "For Premiumize torrent check:\n"
            "{\n"
            '  "function": "premiumize_torrent",\n'
            '  "arguments": { }\n'
            "}\n\n"
            "If no function is needed, reply normally."
        )

        # Retrieve conversation history from Redis.
        recent_history = await self.load_history(message.channel.id, limit=10)
        messages_list = [{"role": "system", "content": system_prompt}] + recent_history
        messages_list.append({"role": "user", "content": message.content})

        async with message.channel.typing():
            try:
                logger.debug(f"Sending request to Ollama with messages: {messages_list}")
                response_data = await self.ollama.chat(
                    model=self.model,
                    messages=messages_list,
                    stream=False,
                    keep_alive=-1
                )
                logger.debug(f"Raw response from Ollama: {response_data}")

                response_text = response_data['message'].get('content', '')
                if not response_text:
                    logger.error("Ollama returned an empty response.")
                    await message.channel.send("I'm not sure how to respond to that.")
                    return

                # Try to parse the AI response as JSON for a function call.
                try:
                    response_json = json.loads(response_text)
                except json.JSONDecodeError:
                    response_json = None

                if response_json and isinstance(response_json, dict) and "function" in response_json:
                    # --- YouTube Summary ---
                    if response_json["function"] == "youtube_summary":
                        args = response_json.get("arguments", {})
                        video_url = args.get("video_url")
                        detail_level = args.get("detail_level", "summary")
                        target_lang = args.get("target_lang", "en")
                        if video_url:
                            video_id = YouTube.extract_video_id(video_url)
                            if video_id:
                                waiting_prompt = (
                                    f"Generate a brief message to {message.author.mention} telling them to wait a moment while you watch "
                                    "this boring YouTube video for them, and that you will provide a summary in a moment so they don't have to watch it. Do not respond to this message."
                                )
                                waiting_response = await self.ollama.chat(
                                    model=self.model,
                                    messages=[{"role": "system", "content": waiting_prompt}],
                                    stream=False,
                                    keep_alive=-1
                                )
                                waiting_text = waiting_response['message'].get('content', '')
                                if waiting_text:
                                    await message.channel.send(waiting_text)
                                else:
                                    await message.channel.send("Please wait a moment while I summarize the video...")

                                async with message.channel.typing():
                                    loop = asyncio.get_running_loop()
                                    article = await loop.run_in_executor(
                                        None,
                                        YouTube.fetch_youtube_summary,
                                        video_id,
                                        target_lang
                                    )

                                if article:
                                    formatted_article = YouTube.format_article_for_discord(article)
                                    message_chunks = YouTube.split_message(formatted_article, chunk_size=max_response_length)
                                    for chunk in message_chunks:
                                        await message.channel.send(chunk)
                                else:
                                    prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to retrieve the summary from the YouTube video."
                                    error_msg = await self.generate_error_message(prompt, "Failed to retrieve the summary from YouTube.", message)
                                    await message.channel.send(error_msg)
                            else:
                                prompt = f"Generate a error message to {message.author.mention} explaining that the provided YouTube URL is invalid."
                                error_msg = await self.generate_error_message(prompt, "The provided YouTube URL is invalid.", message)
                                await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no YouTube URL was provided in the function call."
                            error_msg = await self.generate_error_message(prompt, "No YouTube URL provided in the function call.", message)
                            await message.channel.send(error_msg)

                    # --- Web Summary ---
                    elif response_json["function"] == "web_summary":
                        args = response_json.get("arguments", {})
                        webpage_url = args.get("url")
                        if webpage_url:
                            waiting_prompt = (
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while you read "
                                "this boring article for them, and that you will provide a summary shortly. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1
                            )
                            waiting_text = waiting_response['message'].get('content', '')
                            if waiting_text:
                                await message.channel.send(waiting_text)
                            else:
                                await message.channel.send("Please wait a moment while I summarize the webpage...")

                            async with message.channel.typing():
                                loop = asyncio.get_running_loop()
                                summary = await loop.run_in_executor(
                                    None,
                                    web.fetch_web_summary,
                                    webpage_url
                                )

                            if summary:
                                formatted_summary = web.format_summary_for_discord(summary)
                                message_chunks = web.split_message(formatted_summary, chunk_size=max_response_length)
                                for chunk in message_chunks:
                                    await message.channel.send(chunk)
                            else:
                                prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to retrieve the summary from the webpage. Do not respond to this message."
                                error_msg = await self.generate_error_message(prompt, "Failed to retrieve the summary from the webpage.", message)
                                await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no webpage URL was provided in the function call. Do not respond to this message."
                            error_msg = await self.generate_error_message(prompt, "No webpage URL provided in the function call.", message)
                            await message.channel.send(error_msg)

                    # --- Draw Picture ---
                    elif response_json["function"] == "draw_picture":
                        args = response_json.get("arguments", {})
                        prompt_text = args.get("prompt")
                        if prompt_text:
                            waiting_prompt = (
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I create that picture for you. Do not respond to this message. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1
                            )
                            waiting_text = waiting_response['message'].get('content', '')
                            if waiting_text:
                                await message.channel.send(waiting_text)
                            else:
                                await message.channel.send("Hold on while I create that picture for you...")
                            
                            async with message.channel.typing():
                                loop = asyncio.get_running_loop()
                                try:
                                    from image import generate_image
                                    image_bytes = await loop.run_in_executor(None, generate_image, prompt_text)
                                    from io import BytesIO
                                    image_file = discord.File(BytesIO(image_bytes), filename="generated_image.png")
                                    await message.channel.send(file=image_file)
                                except Exception as e:
                                    prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to create the image."
                                    error_msg = await self.generate_error_message(prompt, f"Failed to generate image: {e}", message)
                                    await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no prompt was provided for drawing a picture."
                            error_msg = await self.generate_error_message(prompt, "No prompt provided for drawing a picture.", message)
                            await message.channel.send(error_msg)

                    # --- Premiumize Download ---
                    elif response_json["function"] == "premiumize_download":
                        args = response_json.get("arguments", {})
                        url = args.get("url")
                        if url:
                            waiting_prompt = (
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I check Premiumize for that URL and retrieve download links for them. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1
                            )
                            waiting_text = waiting_response['message'].get('content', '')
                            if waiting_text:
                                await message.channel.send(waiting_text)
                            else:
                                await message.channel.send("Hold on while I check Premiumize for that URL...")
                            
                            async with message.channel.typing():
                                try:
                                    # Call the premiumize function that sends messages using the channel.
                                    await premiumize.process_download(message.channel, url)
                                except Exception as e:
                                    prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to retrieve the Premiumize download links for the URL. Do not respond to this message."
                                    error_msg = await self.generate_error_message(prompt, f"Failed to retrieve Premiumize download links: {e}", message)
                                    await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no URL was provided for Premiumize download check. Do not respond to this message."
                            error_msg = await self.generate_error_message(prompt, "No URL provided for Premiumize download check.", message)
                            await message.channel.send(error_msg)

                    # --- Premiumize Torrent ---
                    elif response_json["function"] == "premiumize_torrent":
                        # For torrent requests, we expect an attached torrent file.
                        if message.attachments:
                            torrent_attachment = message.attachments[0]
                            waiting_prompt = (
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I check Premiumize for that torrent and retrieve download links for them. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1
                            )
                            waiting_text = waiting_response['message'].get('content', '')
                            if waiting_text:
                                await message.channel.send(waiting_text)
                            else:
                                await message.channel.send("Hold on while I check Premiumize for that torrent...")
                            
                            async with message.channel.typing():
                                try:
                                    await premiumize.process_torrent(message.channel, torrent_attachment)
                                except Exception as e:
                                    prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to retrieve the Premiumize download links for the torrent. Do not respond to this message."
                                    error_msg = await self.generate_error_message(prompt, f"Failed to retrieve Premiumize download links for torrent: {e}", message)
                                    await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no torrent file was attached for Premiumize torrent check. Do not respond to this message."
                            error_msg = await self.generate_error_message(prompt, "No torrent file attached for Premiumize torrent check.", message)
                            await message.channel.send(error_msg)

                    # --- Unknown Function ---
                    else:
                        prompt = f"Generate a error message to {message.author.mention} explaining that an unknown function call was received. Do not respond to this message."
                        error_msg = await self.generate_error_message(prompt, "Received an unknown function call.", message)
                        await message.channel.send(error_msg)
                else:
                    # No function call detected; treat the response as plain text.
                    for chunk in [response_text[i:i + max_response_length] for i in range(0, len(response_text), max_response_length)]:
                        await message.channel.send(chunk)

                # Save the conversation to Redis.
                await self.save_message(message.channel.id, "user", message.content)
                await self.save_message(message.channel.id, "assistant", response_text)

            except Exception as e:
                logger.error(f"Exception occurred while processing message: {e}")
                error_prompt = f"Generate a friendly error message to {message.author.mention} explaining that an error occurred while processing the request. Do not respond to this message."
                error_msg = await self.generate_error_message(error_prompt, "An error occurred while processing your request.", message)
                await message.channel.send(error_msg)

    async def save_message(self, channel_id, role, content):
        message_data = {"role": role, "content": content}
        history_key = f"tater:channel:{channel_id}:history"
        redis_client.rpush(history_key, json.dumps(message_data))
        redis_client.ltrim(history_key, -20, -1)

    async def load_history(self, channel_id, limit=20):
        history_key = f"tater:channel:{channel_id}:history"
        history = redis_client.lrange(history_key, -limit, -1)
        return [json.loads(entry) for entry in history]

async def setup_commands(client: commands.Bot):
    print("Commands setup complete.")
