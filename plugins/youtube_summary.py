import os
import re
import asyncio
import requests
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
import streamlit as st

from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound

from plugin_base import ToolPlugin
from helpers import load_image_from_url, send_waiting_message

load_dotenv()
assistant_avatar = load_image_from_url()


class YouTubeSummaryPlugin(ToolPlugin):
    name = "youtube_summary"
    usage = (
        '{\n'
        '  "function": "youtube_summary",\n'
        '  "arguments": {\n'
        '    "video_url": "<YouTube URL>"\n'
        '  }\n'
        '}'
    )
    description = "Summarizes a YouTube video using its transcript."
    platforms = ["discord", "webui"]
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I watch this boring video and summarize it. Only generate the message. Do not respond to this message."
    )

    @staticmethod
    def extract_video_id(youtube_url):
        parsed = urlparse(youtube_url)
        if parsed.hostname in ['www.youtube.com', 'youtube.com', 'm.youtube.com']:
            return parse_qs(parsed.query).get('v', [None])[0]
        elif parsed.hostname == 'youtu.be':
            return parsed.path.lstrip('/')
        return None

    @staticmethod
    def split_text_into_chunks(text, max_tokens=1200):
        chunks = []
        words = text.split()
        chunk = []
        current_len = 0

        for word in words:
            chunk.append(word)
            current_len += 1
            if current_len >= max_tokens:
                chunks.append(" ".join(chunk))
                chunk = []
                current_len = 0

        if chunk:
            chunks.append(" ".join(chunk))
        return chunks

    def get_transcript_api(self, video_id):
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            return " ".join([seg["text"] for seg in transcript])
        except NoTranscriptFound:
            return None
        except Exception as e:
            print(f"[YouTubeTranscriptApi error] {e}")
            return None

    async def summarize_chunks(self, chunks, ollama_client):
        partial_summaries = []
        for chunk in chunks:
            prompt = (
                "You are summarizing part of a longer YouTube video transcript. "
                "Write a brief summary of this portion using bullet points:\n\n" + chunk
            )
            resp = await ollama_client.chat(
                model=ollama_client.model,
                messages=[{"role": "system", "content": prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": ollama_client.context_length}
            )
            partial_summaries.append(resp["message"]["content"].strip())

        # Combine into final summary
        full_prompt = (
            "This is a set of bullet point summaries from different sections of a YouTube video. "
            "Combine them into a single summary with a title and final bullet points:\n\n" + "\n\n".join(partial_summaries)
        )
        final = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "system", "content": full_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )
        return final["message"]["content"].strip()

    def format_article(self, article):
        return article.replace("### ", "# ")

    def split_message(self, text, chunk_size=1500):
        parts = []
        while len(text) > chunk_size:
            split = text.rfind("\n", 0, chunk_size)
            if split == -1:
                split = text.rfind(" ", 0, chunk_size)
            if split == -1:
                split = chunk_size
            parts.append(text[:split])
            text = text[split:].strip()
        parts.append(text)
        return parts

    async def async_fetch_summary(self, video_url, ollama_client):
        video_id = self.extract_video_id(video_url)
        transcript = self.get_transcript_api(video_id)
        if not transcript:
            return "Sorry, this video does not have a transcript available, or it may be restricted."

        chunks = self.split_text_into_chunks(transcript)
        return await self.summarize_chunks(chunks, ollama_client)

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        video_url = args.get("video_url")
        if not video_url:
            return "No YouTube URL provided."

        prompt = f"Generate a brief message to {message.author.mention} telling them to wait a moment while I summarize this video."
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=prompt,
            save_callback=lambda _: None,
            send_callback=lambda text: asyncio.create_task(self.safe_send(message.channel, text))
        )

        summary = await self.async_fetch_summary(video_url, ollama_client)
        formatted = self.format_article(summary)
        for part in self.split_message(formatted, max_response_length):
            await self.safe_send(message.channel, part)
        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        video_url = args.get("video_url")
        if not video_url:
            return "No YouTube URL provided."

        prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=prompt,
            save_callback=lambda _: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
        )

        summary = await self.async_fetch_summary(video_url, ollama_client)
        return "\n".join(self.split_message(self.format_article(summary)))

    async def safe_send(self, channel, content):
        if len(content) <= 2000:
            await channel.send(content)
        else:
            for chunk in self.split_message(content, 1900):
                await channel.send(chunk)


plugin = YouTubeSummaryPlugin()