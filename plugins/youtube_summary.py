# plugins/youtube_summary.py
import os
import json
import re
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound
import asyncio
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO
import ollama

load_dotenv()
# The assistant avatar will be loaded using the helper.
from helpers import load_image_from_url, send_waiting_message
assistant_avatar = load_image_from_url()

class YouTubeSummaryPlugin(ToolPlugin):
    name = "youtube_summary"
    usage = (
        "{\n"
        '  "function": "youtube_summary",\n'
        '  "arguments": {"video_url": "<YouTube URL>", "target_lang": "<language code (optional)>"}\n'
        "}\n"
    )
    description = "Summarizes a YouTube Video from a URL provided by the user."
    # Waiting prompt template with a placeholder.
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I watch this boring video and summarize it. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui"]

    # --- Helper Functions as Static Methods ---
    @staticmethod
    def extract_video_id(youtube_url):
        parsed_url = urlparse(youtube_url)
        if parsed_url.hostname in ['www.youtube.com', 'youtube.com', 'm.youtube.com']:
            query_params = parse_qs(parsed_url.query)
            return query_params.get('v', [None])[0]
        elif parsed_url.hostname == 'youtu.be':
            return parsed_url.path.lstrip('/')
        return None

    @staticmethod
    def format_article_for_discord(article):
        return article.replace("### ", "# ")

    @staticmethod
    def split_message(message_content, chunk_size=1500):
        message_parts = []
        while len(message_content) > chunk_size:
            split_point = message_content.rfind('\n', 0, chunk_size)
            if split_point == -1:
                split_point = message_content.rfind(' ', 0, chunk_size)
            if split_point == -1:
                split_point = chunk_size
            message_parts.append(message_content[:split_point])
            message_content = message_content[split_point:].strip()
        message_parts.append(message_content)
        return message_parts

    @staticmethod
    def get_transcript(video_id, target_lang=None):
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=[target_lang] if target_lang else [])
        except NoTranscriptFound:
            try:
                transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
                available_languages = [t.language_code for t in transcript_list]
                transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=available_languages)
            except Exception as e:
                print(f"Error fetching transcript: {e}")
                return None
        return " ".join([t['text'] for t in transcript])

    async def async_generate_article(self, transcript, target_lang, ollama_client):
        prompt = "Please summarize the following article. Give it a title and use bullet points:\n\n" + transcript
        if target_lang:
            prompt += f"\n\nWrite the article in {target_lang}."
        response = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "system", "content": prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )
        return response['message'].get('content', '').strip()

    async def async_fetch_youtube_summary(self, video_id, target_lang, ollama_client):
        transcript = await asyncio.to_thread(self.get_transcript, video_id, target_lang)
        if not transcript:
            error_prompt = (
                "Please generate a friendly error message explaining that there was an error processing the request because no transcript is available, and do not respond further."
            )
            response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[{"role": "system", "content": error_prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": ollama_client.context_length}
            )
            return response['message'].get('content', '').strip()
        return await self.async_generate_article(transcript, target_lang, ollama_client)

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        video_url = args.get("video_url")
        target_lang = args.get("target_lang", "en")
        if not video_url:
            return "No YouTube URL provided."
        video_id = self.extract_video_id(video_url)
        if not video_id:
            return "Invalid YouTube URL."

        # Format waiting prompt with user mention.
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )

        summary = await self.async_fetch_youtube_summary(video_id, target_lang, ollama_client)
        if not summary:
            return "Failed to retrieve summary from YouTube."
        formatted_article = self.format_article_for_discord(summary)
        for chunk in self.split_message(formatted_article, chunk_size=max_response_length):
            await message.channel.send(chunk)
        return ""

    # --- Web UI Handler ---
    async def handle_webui(self, args, ollama_client, context_length):
        video_url = args.get("video_url")
        target_lang = args.get("target_lang", "en")
        if not video_url:
            return "No YouTube URL provided."
        video_id = self.extract_video_id(video_url)
        if not video_id:
            return "Invalid YouTube URL."

        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
        )

        summary = await self.async_fetch_youtube_summary(video_id, target_lang, ollama_client)
        if not summary:
            return "Failed to retrieve summary from YouTube."
        formatted_article = self.format_article_for_discord(summary)
        return "\n".join(self.split_message(formatted_article))
        
# Export an instance.
plugin = YouTubeSummaryPlugin()