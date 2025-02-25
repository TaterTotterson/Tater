# tater.py
import os
import json
import asyncio
import logging
import redis
import discord
from discord.ext import commands
import ollama
from embed import generate_embedding, save_embedding, find_relevant_context
from dotenv import load_dotenv
import re
import YouTube  # Module for YouTube summarization functions
import web      # Module for webpage summarization functions
import premiumize  # Module for Premiumize-related functions
from search import search_web, format_search_results

# Load environment variables
load_dotenv()
ollama_model = os.getenv('OLLAMA_MODEL', 'llama3.2').strip()
response_channel_id = int(os.getenv("RESPONSE_CHANNEL_ID", 0))
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
max_response_length = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))


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
                keep_alive=-1,
                options={"num_ctx": context_length}
            )
            error_text = error_response['message'].get('content', '').strip()
            if error_text:
                return error_text
        except Exception as e:
            logger.error(f"Error generating error message: {e}")
        return fallback

    async def on_message(self, message: discord.Message):
        # 1) Ignore messages from the bot itself.
        if message.author == self.user:
            return

        # 2) Check if we're in a DM (private message).
        #    - If yes and the user is not admin, ignore the message.
        #    - If yes and the user is admin, respond.
        #    - Otherwise, fall back to channel-based logic.
        if isinstance(message.channel, discord.DMChannel):
            # This is a direct message
            if message.author.id == int(os.getenv("ADMIN_USER_ID", 0)):
                # Admin in DMs => respond without needing a mention
                should_respond = True
            else:
                # Non-admin in DMs => ignore
                return
        else:
            # Not a DM, so do the existing logic:
            # - respond if in RESPONSE_CHANNEL_ID or bot is mentioned
            should_respond = (
                message.channel.id == response_channel_id
                or self.user.mentioned_in(message)
            )
            if not should_respond:
                return
        
        # Ensure embedding is always initialized before use
        embedding = None  

        # Check if message length is at least 30 characters before storing
        if len(message.content.strip()) >= 30:
            embedding = await generate_embedding(message.content)
            if embedding:
                await save_embedding(message.content, embedding)

        # Retrieve relevant context from past messages
        if embedding is not None:
            relevant_context = await find_relevant_context(embedding)
        else:
            relevant_context = []

        # Log the retrieved context
        if relevant_context:
            logger.debug("Retrieved relevant context:")
            for idx, text in enumerate(relevant_context, 1):
                logger.debug(f"{idx}. {text}")
        else:
            logger.debug("No relevant context found.")

        # Build a system prompt with the relevant context
        system_prompt = (
            "You are Tater Totterson, a retro gaming enthusiast who is part of the DNServ Crew.\n\n"
            "The DNServ Crew is an elite, tight-knit Retro Gaming group. You help the DNServ Crew with various tools. If you need real-time access to the internet or don't have any information about what the user is asking use the 'web_search' tool, Dont ask to use a tool just use it.\n\n"
            "You have access to the following tools:\n\n"
            "1. 'youtube_summary' for summarizing YouTube videos. Pretend you have to watch the entire video to produce an accurate summary.\n\n"
            "2. 'web_summary' for summarizing news articles or webpage text. Pretend you have to read the whole article to create a proper summary.\n\n"
            "3. 'draw_picture' for generating images. If a user asks you to draw a picture, use this tool and pretend you are drawing the picture yourself.\n\n"
            "4. 'premiumize_download' for checking if a URL is cached on Premiumize.me and retrieving download links.\n\n"
            "5. 'premiumize_torrent' for checking if a torrent file is cached on Premiumize.me and retrieving download links.\n\n"
            "6. 'watch_feed' for adding a new RSS feed to monitor. When a new article appears in the feed, the bot will summarize it and announce the news.\n\n"
            "7. 'unwatch_feed' for stopping the monitoring of an RSS feed.\n\n"
            "8. 'list_feeds' for listing all currently watched RSS feeds.\n\n"
            "9. 'web_search' for searching the web when additional or up-to-date information is needed to answer a user's question. If you are unsure of the answer, lack sufficient context, or your internal knowledge is outdated, use this tool to retrieve current information from the web.\n\n"
            "When a user requests one of these actions or you need real-time access to the internet, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
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
            "For adding an RSS feed to watch:\n"
            "{\n"
            '  "function": "watch_feed",\n'
            '  "arguments": {\n'
            '      "feed_url": "<RSS feed URL>"\n'
            "  }\n"
            "}\n\n"
            "For stopping the watch on an RSS feed:\n"
            "{\n"
            '  "function": "unwatch_feed",\n'
            '  "arguments": {\n'
            '      "feed_url": "<RSS feed URL>"\n'
            "  }\n"
            "}\n\n"
            "For listing all watched RSS feeds:\n"
            "{\n"
            '  "function": "list_feeds",\n'
            '  "arguments": { }\n'
            "}\n\n"
            "{\n"
            '  "function": "web_search",\n'
            '  "arguments": {\n'
            '      "query": "<search query>"\n'
            "  }\n"
            "}\n\n"

            "If no function is needed, reply normally."
        )

        # Add relevant context from stored global knowledge
        if relevant_context:
            context_prompt = "Here is some relevant information retrieved from previously stored knowledge:\n"
            for text in relevant_context:
                context_prompt += f"- {text}\n"
            system_prompt += "\n\n" + context_prompt

        # Retrieve conversation history from Redis.
        recent_history = await self.load_history(message.channel.id, limit=20)
        messages_list = [{"role": "system", "content": system_prompt}] + recent_history
        messages_list.append({"role": "user", "content": f"{message.author.name}: {message.content}"})

        async with message.channel.typing():
            try:
                logger.debug(f"Sending request to Ollama with messages: {messages_list}")
                response_data = await self.ollama.chat(
                    model=self.model,
                    messages=messages_list,
                    stream=False,
                    keep_alive=-1,
                    options={"num_ctx": context_length}
                )
                logger.debug(f"Raw response from Ollama: {response_data}")

                response_text = response_data['message'].get('content', '').strip()

                if not response_text:
                    logger.error("Ollama returned an empty response.")
                    await message.channel.send("I'm not sure how to respond to that.")
                    return

                # Generate embedding for bot response, but only store if it's useful
                if len(response_text) >= 30:  # Ensures only meaningful bot responses are stored
                    response_embedding = await generate_embedding(response_text)
                    if response_embedding:
                        await save_embedding(response_text, response_embedding)
                        logger.info("Bot response saved")
                else:
                    logger.info("Bot response NOT saved (too short)")

                # Try to parse the AI response as JSON for a function call.
                try:
                    response_json = json.loads(response_text)
                except json.JSONDecodeError:
                    # Attempt to extract JSON substring if there is extra text
                    json_start = response_text.find('{')
                    json_end = response_text.rfind('}')
                    if json_start != -1 and json_end != -1:
                        json_str = response_text[json_start:json_end+1]
                        try:
                            response_json = json.loads(json_str)
                        except Exception as e:
                            response_json = None
                    else:
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
                                    "this boring YouTube video for them, and that you will provide a summary in a moment so they don't have to watch it. Only generate the message. Do not respond to this message."
                                )
                                waiting_response = await self.ollama.chat(
                                    model=self.model,
                                    messages=[{"role": "system", "content": waiting_prompt}],
                                    stream=False,
                                    keep_alive=-1,
                                    options={"num_ctx": context_length}
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
                                "this boring article for them, and that you will provide a summary shortly. Only generate the message. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1,
                                options={"num_ctx": context_length}
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
                                prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to retrieve the summary from the webpage. Only generate the message. Do not respond to this message."
                                error_msg = await self.generate_error_message(prompt, "Failed to retrieve the summary from the webpage.", message)
                                await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no webpage URL was provided in the function call. Only generate the message. Do not respond to this message."
                            error_msg = await self.generate_error_message(prompt, "No webpage URL provided in the function call.", message)
                            await message.channel.send(error_msg)

                    # --- Draw Picture ---
                    elif response_json["function"] == "draw_picture":
                        args = response_json.get("arguments", {})
                        prompt_text = args.get("prompt")
                        if prompt_text:
                            waiting_prompt = (
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I create that picture for you. Only generate the message. Do not respond to this message. Only generate the message. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1,
                                options={"num_ctx": context_length}
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
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I check Premiumize for that URL and retrieve download links for them. Only generate the message. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1,
                                options={"num_ctx": context_length}
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
                                    prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to retrieve the Premiumize download links for the URL. Only generate the message. Do not respond to this message."
                                    error_msg = await self.generate_error_message(prompt, f"Failed to retrieve Premiumize download links: {e}", message)
                                    await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no URL was provided for Premiumize download check. Only generate the message. Do not respond to this message."
                            error_msg = await self.generate_error_message(prompt, "No URL provided for Premiumize download check.", message)
                            await message.channel.send(error_msg)

                    # --- Premiumize Torrent ---
                    elif response_json["function"] == "premiumize_torrent":
                        # For torrent requests, we expect an attached torrent file.
                        if message.attachments:
                            torrent_attachment = message.attachments[0]
                            waiting_prompt = (
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I check Premiumize for that torrent and retrieve download links for them. Only generate the message. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
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
                                try:
                                    await premiumize.process_torrent(message.channel, torrent_attachment)
                                except Exception as e:
                                    prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to retrieve the Premiumize download links for the torrent. Only generate the message. Do not respond to this message."
                                    error_msg = await self.generate_error_message(prompt, f"Failed to retrieve Premiumize download links for torrent: {e}", message)
                                    await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a error message to {message.author.mention} explaining that no torrent file was attached for Premiumize torrent check. Only generate the message. Do not respond to this message."
                            error_msg = await self.generate_error_message(prompt, "No torrent file attached for Premiumize torrent check.", message)
                            await message.channel.send(error_msg)

                    # --- Watch Feed ---
                    elif response_json["function"] == "watch_feed":
                        args = response_json.get("arguments", {})
                        feed_url = args.get("feed_url")
                        if feed_url:
                            if hasattr(self, "rss_manager") and self.rss_manager is not None:
                                success = self.rss_manager.add_feed(feed_url)
                                if success:
                                    # Generate confirmation message via Ollama
                                    prompt = (f"Generate a friendly confirmation message to {message.author.mention} "
                                              f"that the feed '{feed_url}' has been successfully added and is now being watched. Only generate the message. Do not respond to this message.")
                                    generated = await self.ollama.chat(
                                        model=self.model,
                                        messages=[{"role": "system", "content": prompt}],
                                        stream=False,
                                        keep_alive=-1,
                                        options={"num_ctx": context_length}
                                    )
                                    confirmation_text = generated['message'].get('content', '').strip()
                                    if confirmation_text:
                                        await message.channel.send(confirmation_text)
                                    else:
                                        await message.channel.send(f"✅ Now watching feed: {feed_url}")
                                else:
                                    prompt = (f"Generate an error message to {message.author.mention} "
                                              f"explaining that the provided RSS feed URL could not be added. Only generate the message. Do not respond to this message.")
                                    error_msg = await self.generate_error_message(prompt, f"Failed to add feed: {feed_url}", message)
                                    await message.channel.send(error_msg)
                            else:
                                await message.channel.send("RSS Manager is not initialized.")
                        else:
                            prompt = (f"Generate an error message to {message.author.mention} "
                                      f"explaining that no feed URL was provided for watching. Only generate the message. Do not respond to this message.")
                            error_msg = await self.generate_error_message(prompt, "No feed URL provided for watch_feed.", message)
                            await message.channel.send(error_msg)
                        return

                    # --- Unwatch Feed ---
                    elif response_json["function"] == "unwatch_feed":
                        args = response_json.get("arguments", {})
                        feed_url = args.get("feed_url")
                        if feed_url:
                            if hasattr(self, "rss_manager") and self.rss_manager is not None:
                                success = self.rss_manager.remove_feed(feed_url)
                                if success:
                                    prompt = (f"Generate a friendly confirmation message to {message.author.mention} "
                                              f"that the feed '{feed_url}' has been successfully removed and is no longer being watched. Only generate the message. Do not respond to this message.")
                                    generated = await self.ollama.chat(
                                        model=self.model,
                                        messages=[{"role": "system", "content": prompt}],
                                        stream=False,
                                        keep_alive=-1,
                                        options={"num_ctx": context_length}
                                    )
                                    confirmation_text = generated['message'].get('content', '').strip()
                                    if confirmation_text:
                                        await message.channel.send(confirmation_text)
                                    else:
                                        await message.channel.send(f"✅ Stopped watching feed: {feed_url}")
                                else:
                                    prompt = (f"Generate an error message to {message.author.mention} "
                                              f"explaining that the provided RSS feed URL could not be removed. Only generate the message. Do not respond to this message.")
                                    error_msg = await self.generate_error_message(prompt, f"Failed to remove feed: {feed_url}", message)
                                    await message.channel.send(error_msg)
                            else:
                                await message.channel.send("RSS Manager is not initialized.")
                        else:
                            prompt = (f"Generate an error message to {message.author.mention} "
                                      f"explaining that no feed URL was provided for unwatching. Only generate the message. Do not respond to this message.")
                            error_msg = await self.generate_error_message(prompt, "No feed URL provided for unwatch_feed.", message)
                            await message.channel.send(error_msg)
                        return

                    # --- List Feeds ---
                    elif response_json["function"] == "list_feeds":
                        if hasattr(self, "rss_manager") and self.rss_manager is not None:
                            feeds = self.rss_manager.get_feeds()  # returns a dict: feed_url -> last_seen timestamp
                            if feeds:
                                feed_list = "\n".join(
                                    f"{feed_url} (last update: {feeds[feed_url]})" for feed_url in feeds
                                )
                                prompt = (f"Generate a friendly message to {message.author.mention} "
                                          f"listing the currently watched RSS feeds:\n{feed_list}. Only generate the message. Do not respond to this message.")
                                generated = await self.ollama.chat(
                                    model=self.model,
                                    messages=[{"role": "system", "content": prompt}],
                                    stream=False,
                                    keep_alive=-1,
                                    options={"num_ctx": context_length}
                                )
                                response_text = generated['message'].get('content', '').strip()
                                if response_text:
                                    await message.channel.send(response_text)
                                else:
                                    await message.channel.send(f"**Watched RSS Feeds:**\n{feed_list}")
                            else:
                                prompt = (f"Generate a friendly message to {message.author.mention} "
                                          f"explaining that no RSS feeds are currently being watched. Only generate the message. Do not respond to this message.")
                                generated = await self.ollama.chat(
                                    model=self.model,
                                    messages=[{"role": "system", "content": prompt}],
                                    stream=False,
                                    keep_alive=-1,
                                    options={"num_ctx": context_length}
                                )
                                response_text = generated['message'].get('content', '').strip()
                                if response_text:
                                    await message.channel.send(response_text)
                                else:
                                    await message.channel.send("No RSS feeds are currently being watched.")
                        else:
                            await message.channel.send("RSS Manager is not initialized.")
                        return

                    # --- Web Search ---
                    elif response_json and response_json.get("function") == "web_search":
                        args = response_json.get("arguments", {})
                        query = args.get("query")
                        if query:
                            # Send a waiting prompt to the user.
                            waiting_prompt = (
                                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I search the web for additional information. Only generate the message. Do not respond to this message."
                            )
                            waiting_response = await self.ollama.chat(
                                model=self.model,
                                messages=[{"role": "system", "content": waiting_prompt}],
                                stream=False,
                                keep_alive=-1,
                                options={"num_ctx": context_length}
                            )
                            waiting_text = waiting_response['message'].get('content', '').strip()
                            if waiting_text:
                                await message.channel.send(waiting_text)
                            else:
                                await message.channel.send("Please wait a moment while I search the web...")

                            # Search the web using our tool.
                            results = search_web(query)
                            if results:
                                formatted_results = format_search_results(results)
                                # Build the choice prompt with actual values filled in.
                                choice_prompt = (
                                    f"You are looking for more information on '{query}' because the user asked: '{message.content}'.\n\n"
                                    f"Here are the top search results:\n\n"
                                    f"{formatted_results}\n\n"
                                    "Please choose the most relevant link. Use the following tool for fetching web details and insert the chosen link. "
                                    "Respond ONLY with a valid JSON object in the following exact format (and nothing else):\n\n"
                                    "For fetching web details:\n"
                                    "{\n"
                                    '  "function": "web_fetch",\n'
                                    '  "arguments": {\n'
                                    '      "link": "<chosen link>",\n'
                                    f'      "query": "{query}",\n'
                                    f'      "user_question": "{message.content}"\n'
                                    "  }\n"
                                    "}"
                                )
                                # Call the model with the new prompt.
                                choice_response = await self.ollama.chat(
                                    model=self.model,
                                    messages=[{"role": "system", "content": choice_prompt}],
                                    stream=False,
                                    keep_alive=-1,
                                    options={"num_ctx": context_length}
                                )
                                choice_text = choice_response['message'].get('content', '').strip()
                                # Attempt to parse the response JSON.
                                try:
                                    choice_json = json.loads(choice_text)
                                except json.JSONDecodeError:
                                    json_start = choice_text.find('{')
                                    json_end = choice_text.rfind('}')
                                    if json_start != -1 and json_end != -1:
                                        json_str = choice_text[json_start:json_end+1]
                                        try:
                                            choice_json = json.loads(json_str)
                                        except Exception as e:
                                            choice_json = None
                                    else:
                                        choice_json = None

                                if not choice_json:
                                    prompt = f"Generate a friendly error message to {message.author.mention} explaining that I failed to parse the search result choice. Only generate the message. Do not respond to this message."
                                    error_msg = await self.generate_error_message(prompt, "Failed to parse the search result choice.", message)
                                    await message.channel.send(error_msg)
                                    return

                                if choice_json.get("function") == "web_fetch":
                                    # Process the web_fetch immediately.
                                    args = choice_json.get("arguments", {})
                                    link = args.get("link")
                                    original_query = args.get("query")
                                    user_question = args.get("user_question")
                                    if link:
                                        import web  # Assuming web.py is in the same project
                                        summary = await asyncio.to_thread(web.fetch_web_summary, link)
                                        if summary:
                                            # Build a new prompt instructing the model to use the detailed info to answer the original query.
                                            info_prompt = (
                                                f"Using the detailed information from the selected page below, please provide a clear and concise answer to the original query.\n\n"
                                                f"Original Query: '{original_query}'\n"
                                                f"User Question: '{user_question}'\n\n"
                                                f"Detailed Information:\n{summary}\n\n"
                                                "Answer:"
                                            )
                                            # Call the model to generate the final answer.
                                            final_response = await self.ollama.chat(
                                                model=self.model,
                                                messages=[{"role": "system", "content": info_prompt}],
                                                stream=False,
                                                keep_alive=-1,
                                                options={"num_ctx": context_length}
                                            )
                                            final_answer = final_response['message'].get('content', '').strip()
                                            if final_answer:
                                                # Split the final answer if it's too long.
                                                if len(final_answer) > max_response_length:
                                                    chunks = web.split_message(final_answer, chunk_size=max_response_length)
                                                    for chunk in chunks:
                                                        await message.channel.send(chunk)
                                                else:
                                                    await message.channel.send(final_answer)
                                            else:
                                                prompt = f"Generate a friendly error message to {message.author.mention} explaining that I failed to generate a final answer from the detailed info. Only generate the message. Do not respond to this message."
                                                error_msg = await self.generate_error_message(prompt, "Failed to generate a final answer from the detailed info.", message)
                                                await message.channel.send(error_msg)
                                        else:
                                            prompt = f"Generate a friendly error message to {message.author.mention} explaining that I failed to extract information from the selected webpage. Only generate the message. Do not respond to this message."
                                            error_msg = await self.generate_error_message(prompt, "Failed to extract information from the selected webpage.", message)
                                            await message.channel.send(error_msg)
                                    else:
                                        prompt = f"Generate a friendly error message to {message.author.mention} explaining that no link was provided to fetch web info. Only generate the message. Do not respond to this message."
                                        error_msg = await self.generate_error_message(prompt, "No link provided to fetch web info.", message)
                                        await message.channel.send(error_msg)
                                    return
                                else:
                                    prompt = f"Generate a friendly error message to {message.author.mention} explaining that no valid function call for fetching web info was returned. Only generate the message. Do not respond to this message."
                                    error_msg = await self.generate_error_message(prompt, "No valid function call for fetching web info was returned.", message)
                                    await message.channel.send(error_msg)
                                    return
                            else:
                                prompt = f"Generate a friendly error message to {message.author.mention} explaining that I couldn't find any relevant search results."
                                error_msg = await self.generate_error_message(prompt, "I couldn't find any relevant search results.", message)
                                await message.channel.send(error_msg)
                        else:
                            prompt = f"Generate a friendly error message to {message.author.mention} explaining that no search query was provided. Only generate the message. Do not respond to this message."
                            error_msg = await self.generate_error_message(prompt, "No search query provided.", message)
                            await message.channel.send(error_msg)
                        return

                    # --- Unknown Function ---
                    else:
                        prompt = f"Generate a error message to {message.author.mention} explaining that an unknown function call was received. Only generate the message. Do not respond to this message."
                        error_msg = await self.generate_error_message(prompt, "Received an unknown function call.", message)
                        await message.channel.send(error_msg)
                else:
                    # No function call detected; treat the response as plain text.
                    for chunk in [response_text[i:i + max_response_length] for i in range(0, len(response_text), max_response_length)]:
                        await message.channel.send(chunk)

                # Save the conversation to Redis.
                await self.save_message(message.channel.id, "user", message.author.name, message.content)
                await self.save_message(message.channel.id, "assistant", "assistant", response_text)

            except Exception as e:
                logger.error(f"Exception occurred while processing message: {e}")
                error_prompt = f"Generate a friendly error message to {message.author.mention} explaining that an error occurred while processing the request. Only generate the message. Do not respond to this message."
                error_msg = await self.generate_error_message(error_prompt, "An error occurred while processing your request.", message)
                await message.channel.send(error_msg)

    async def save_message(self, channel_id, role, username, content):
        message_data = {"role": role, "username": username, "content": content}
        history_key = f"tater:channel:{channel_id}:history"
        redis_client.rpush(history_key, json.dumps(message_data))
        redis_client.ltrim(history_key, -20, -1)

    async def load_history(self, channel_id, limit=20):
        history_key = f"tater:channel:{channel_id}:history"
        raw_history = redis_client.lrange(history_key, -limit, -1)
        formatted_history = []
        for entry in raw_history:
            data = json.loads(entry)
            role = data.get("role", "user")
            sender = data.get("username", role)
            if role == "assistant":
                formatted_message = data["content"]
            else:
                formatted_message = f"{sender}: {data['content']}"
            formatted_history.append({"role": role, "content": formatted_message})
        return formatted_history

async def setup_commands(client: commands.Bot):
    print("Commands setup complete.")
