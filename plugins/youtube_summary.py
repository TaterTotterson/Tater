# plugins/youtube_summary.py
import os
import re
import time
import asyncio
import subprocess
import requests
import json
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
import streamlit as st
from youtube_transcript_api import YouTubeTranscriptApi
from plugin_base import ToolPlugin
from helpers import redis_client
import redis
import discord

load_dotenv()
DEFAULT_MAX_TOKENS = 2048

class YouTubeSummaryPlugin(ToolPlugin):
    name = "youtube_summary"
    plugin_name = "YouTube Summary"
    usage = (
        '{\n'
        '  "function": "youtube_summary",\n'
        '  "arguments": {\n'
        '    "video_url": "<YouTube URL>"\n'
        '  }\n'
        '}'
    )
    description = "Summarizes a YouTube video using its transcript."
    plugin_dec = "Summarize a YouTube video using its transcript."
    pretty_name = "Summarizing Your Video"
    settings_category = "YouTube Summary"
    required_settings = {
        "update_transcript_api": {
            "type": "button",
            "label": "Update YouTubeTranscriptApi",
            "description": "Manually check and install the latest version of the transcript API."
        }
    }
    waiting_prompt_template = "Write a friendly message telling {mention} you’re watching the video and working on it now! Only output that message."
    platforms = ["discord", "webui", "irc", "matrix"]

    def handle_setting_button(self, key):
        if key == "update_transcript_api":
            try:
                subprocess.run(["pip", "install", "--upgrade", "youtube-transcript-api"], check=True)
                return "Successfully updated youtube-transcript-api."
            except subprocess.CalledProcessError as e:
                return f"Failed to update: {e}"

    @staticmethod
    def extract_video_id(youtube_url):
        parsed = urlparse(youtube_url)
        if parsed.hostname in ['www.youtube.com', 'youtube.com', 'm.youtube.com']:
            return parse_qs(parsed.query).get('v', [None])[0]
        elif parsed.hostname == 'youtu.be':
            return parsed.path.lstrip('/')
        return None

    @staticmethod
    def split_text_into_chunks(text, max_tokens=DEFAULT_MAX_TOKENS):
        limit = int(max_tokens * 0.8)
        words = text.split()
        chunks, chunk = [], []
        for word in words:
            chunk.append(word)
            if len(chunk) >= limit:
                chunks.append(" ".join(chunk))
                chunk = []
        if chunk:
            chunks.append(" ".join(chunk))
        return chunks

    async def safe_send(self, channel, content):
        if len(content) <= 2000:
            await channel.send(content)
        else:
            for chunk in self.split_message(content, 1900):
                await channel.send(chunk)

    def get_transcript_api(self, video_id):
        from youtube_transcript_api import YouTubeTranscriptApi
        import logging

        logger = logging.getLogger("youtube_summary")

        api = YouTubeTranscriptApi()
        try:
            # 1) Try direct fetch
            try:
                fetched = api.fetch(video_id)
            except Exception as e1:
                logger.info(f"[fetch default] {e1}; trying languages…")
                # 2) Prefer English variants; else list → pick → fetch
                try:
                    fetched = api.fetch(video_id, languages=["en", "en-US", "en-GB"])
                except Exception as e2:
                    logger.info(f"[fetch en*] {e2}; listing transcripts…")
                    tl = api.list(video_id)
                    try:
                        t = tl.find_transcript(["en", "en-US", "en-GB"])
                    except Exception:
                        t = next(iter(tl), None)
                    if not t:
                        logger.info("No transcripts available for this video.")
                        return None
                    fetched = t.fetch()

            raw = fetched.to_raw_data() if hasattr(fetched, "to_raw_data") else \
                  [{"text": getattr(sn, "text", "")} for sn in fetched]
            text = " ".join(seg.get("text", "") for seg in raw if seg.get("text"))
            return text.strip() or None

        except Exception as e:
            logger.error(f"[YouTubeTranscriptApi fatal] {e}", exc_info=True)
            return None

    async def summarize_chunks(self, chunks, llm_client):
        partial_summaries = []
        for chunk in chunks:
            prompt = (
                "You are summarizing part of a longer YouTube video transcript. "
                "Write a brief summary of this portion using bullet points:\n\n" + chunk
            )
            resp = await llm_client.chat(
                messages=[{"role": "system", "content": prompt}]
            )
            partial_summaries.append(resp["message"]["content"].strip())

        final_prompt = (
            "This is a set of bullet point summaries from different sections of a YouTube video. "
            "Combine them into a single summary with a title and final bullet points:\n\n"
            + "\n\n".join(partial_summaries)
        )
        final = await llm_client.chat(
            messages=[{"role": "system", "content": final_prompt}]
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

    async def _fetch_summary(self, video_url, llm_client):
        video_id = self.extract_video_id(video_url)
        transcript = self.get_transcript_api(video_id)
        if not transcript:
            return "Sorry, this video does not have a transcript available, or it may be restricted."
        chunks = self.split_text_into_chunks(transcript, DEFAULT_MAX_TOKENS)
        return await self.summarize_chunks(chunks, llm_client)

    # ---------------------------------------------------------
    # Discord handler
    # ---------------------------------------------------------
    async def handle_discord(self, message, args, llm_client):
        video_url = args.get("video_url")
        if not video_url:
            return "No YouTube URL provided."

        try:
            loop = asyncio.get_running_loop()
            summary = await self._fetch_summary(video_url, llm_client)
        except RuntimeError:
            summary = asyncio.run(self._fetch_summary(video_url, llm_client))

        formatted = self.format_article(summary)
        return "\n".join(self.split_message(formatted, 1500))

    # ---------------------------------------------------------
    # WebUI handler
    # ---------------------------------------------------------
    async def handle_webui(self, args, llm_client):
        video_url = args.get("video_url")
        if not video_url:
            return ["No YouTube URL provided."]

        try:
            loop = asyncio.get_running_loop()
            summary = await self._fetch_summary(video_url, llm_client)
        except RuntimeError:
            summary = asyncio.run(self._fetch_summary(video_url, llm_client))

        if not summary:
            return ["Failed to summarize the video."]

        formatted = self.format_article(summary)
        return self.split_message(formatted, 1500)

    # ---------------------------------------------------------
    # IRC handler
    # ---------------------------------------------------------
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        video_url = args.get("video_url")
        nick = user

        if not video_url:
            return f"{nick}: No YouTube URL provided."

        try:
            loop = asyncio.get_running_loop()
            summary = await self._fetch_summary(video_url, llm_client)
        except RuntimeError:
            summary = asyncio.run(self._fetch_summary(video_url, llm_client))

        if not summary:
            return f"{nick}: Could not generate summary."

        article = self.format_article(summary)
        return f"{nick}: {article}"

    # ---------------------------------------------------------
    # Matrix handler
    # ---------------------------------------------------------
    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        llm = llm_client or kwargs.get("llm")
        video_url = (args or {}).get("video_url")
        if not video_url:
            return "No YouTube URL provided."

        try:
            summary = await self._fetch_summary(video_url, llm)
        except Exception as e:
            return f"Failed to summarize the video: {e}"

        if not summary:
            return "Failed to summarize the video."

        # Matrix platform will handle chunking; return one string.
        return self.format_article(summary)

plugin = YouTubeSummaryPlugin()
