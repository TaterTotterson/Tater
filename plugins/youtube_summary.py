import os
import re
import asyncio
import tempfile
import json
import requests
from urllib.parse import urlparse, parse_qs
from plugin_base import ToolPlugin
from dotenv import load_dotenv
from yt_dlp import YoutubeDL
import streamlit as st
from helpers import load_image_from_url, send_waiting_message

load_dotenv()
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
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I watch this boring video and summarize it. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui"]

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
    def get_transcript_ytdlp(video_url, target_lang='en'):
        ydl_opts = {
            'skip_download': True,
            'writesubtitles': True,
            'writeautomaticsub': True,
            'quiet': True,
            'no_warnings': True,
        }

        with YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(video_url, download=False)
                # Try exact subtitle match
                subtitles = info.get("subtitles", {}) or {}
                auto_subs = info.get("automatic_captions", {}) or {}

                # Look for manual or auto subs in preferred language
                for source in [subtitles, auto_subs]:
                    if target_lang in source:
                        formats = source[target_lang]
                        # Prefer vtt or first format
                        sub_url = next((f['url'] for f in formats if f['ext'] == 'vtt'), formats[0]['url'])

                        return YouTubeSummaryPlugin.fetch_vtt_text(sub_url), target_lang

                # Fallback: first available language
                for source in [subtitles, auto_subs]:
                    for lang, formats in source.items():
                        sub_url = next((f['url'] for f in formats if f['ext'] == 'vtt'), formats[0]['url'])
                        return YouTubeSummaryPlugin.fetch_vtt_text(sub_url), lang
            except Exception as e:
                print(f"[yt-dlp error] {e}")
        return None, None

    @staticmethod
    def fetch_vtt_text(url):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                lines = resp.text.splitlines()
                text_lines = [line.strip() for line in lines if line and not line.startswith("WEBVTT") and not re.match(r"^\d\d:\d\d:\d\d\.\d\d\d", line)]
                return " ".join(text_lines)
        except Exception as e:
            print(f"[vtt fetch error] {e}")
        return ""


    async def async_fetch_youtube_summary(self, video_url, target_lang, ollama_client):
        transcript, transcript_lang = await asyncio.to_thread(self.get_transcript_ytdlp, video_url, target_lang)
        if not transcript or transcript.strip() == "":
            return "Sorry, no transcript could be retrieved from this video."

        if transcript_lang != target_lang:
            prompt = (
                f"The following transcript is in {transcript_lang}. "
                f"Please summarize the following transcript in {target_lang}. Give it a title and use bullet points:\n\n{transcript}"
            )
        else:
            prompt = (
                "Please summarize the following transcript. Give it a title and use bullet points:\n\n" + transcript
            )

        response = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "system", "content": prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )
        return response['message'].get('content', '').strip()

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        video_url = args.get("video_url")
        target_lang = args.get("target_lang", "en")
        if not video_url:
            return "No YouTube URL provided."
        if not self.extract_video_id(video_url):
            return "Invalid YouTube URL."

        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )

        summary = await self.async_fetch_youtube_summary(video_url, target_lang, ollama_client)
        formatted_article = self.format_article_for_discord(summary)
        for chunk in self.split_message(formatted_article, chunk_size=max_response_length):
            await message.channel.send(chunk)
        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        video_url = args.get("video_url")
        target_lang = args.get("target_lang", "en")
        if not video_url:
            return "No YouTube URL provided."
        if not self.extract_video_id(video_url):
            return "Invalid YouTube URL."

        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
        )

        summary = await self.async_fetch_youtube_summary(video_url, target_lang, ollama_client)
        formatted_article = self.format_article_for_discord(summary)
        return "\n".join(self.split_message(formatted_article))

plugin = YouTubeSummaryPlugin()