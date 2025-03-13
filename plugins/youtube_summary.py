# plugins/youtube_summary.py
import os
import json
import requests
import re
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound
import asyncio
from plugin_base import ToolPlugin
import streamlit as st
import requests
from PIL import Image
from io import BytesIO
import ollama

# Load environment variables
load_dotenv()
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_SERVER = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral-small:24b")
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Load the assistant avatar from URL using requests and Pillow.
def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

class YouTubeSummaryPlugin(ToolPlugin):
    name = "youtube_summary"
    usage = (
        "{\n"
        '  "function": "youtube_summary",\n'
        '  "arguments": {"video_url": "<YouTube URL>"}\n'
        "}\n"
    )
    description = "Summarizes a You Tube Video from a URL provided by the user."
    platforms = ["discord", "webui"]

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        video_url = args.get("video_url")
        target_lang = args.get("target_lang", "en")
        if not video_url:
            return "No YouTube URL provided."
        video_id = extract_video_id(video_url)
        if not video_id:
            return "Invalid YouTube URL."
        
        waiting_prompt = (
            f"Generate a brief message to {message.author.mention} telling them to wait a moment while you watch this boring video for them and summarize the video. Only generate the message. Do not respond to this message."
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
            await message.channel.send("Please wait a moment while I summarize the video...")

        async with message.channel.typing():
            loop = asyncio.get_running_loop()
            article = await loop.run_in_executor(
                None,
                fetch_youtube_summary,
                video_id,
                target_lang
            )
        if not article:
            return "Failed to retrieve summary from YouTube."
        formatted_article = format_article_for_discord(article)
        message_chunks = split_message(formatted_article, chunk_size=max_response_length)
        for chunk in message_chunks:
            await message.channel.send(chunk)
        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        video_url = args.get("video_url")
        target_lang = args.get("target_lang", "en")
        if not video_url:
            return "No YouTube URL provided."
        video_id = extract_video_id(video_url)
        if not video_id:
            return "Invalid YouTube URL."
        
        # Send a waiting message to the user in the web UI.
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while you watch this boring video for them and summarize it. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '').strip()
        if waiting_text:
            st.chat_message("assistant", avatar=assistant_avatar).write(waiting_text)
        else:
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait a moment while I summarize the video...")
        
        # Generate the summary asynchronously.
        article = await asyncio.to_thread(fetch_youtube_summary, video_id, target_lang)
        if not article:
            return "Failed to retrieve summary from YouTube."
        
        formatted_article = format_article_for_discord(article)
        chunks = split_message(formatted_article)
        final_response = "\n".join(chunks)
        
        return final_response

# Export an instance of the plugin.
plugin = YouTubeSummaryPlugin()


def extract_video_id(youtube_url):
    parsed_url = urlparse(youtube_url)
    if parsed_url.hostname in ['www.youtube.com', 'youtube.com', 'm.youtube.com']:
        query_params = parse_qs(parsed_url.query)
        return query_params.get('v', [None])[0]
    elif parsed_url.hostname == 'youtu.be':
        return parsed_url.path.lstrip('/')
    return None

def format_article_for_discord(article):
    return article.replace("### ", "# ")

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
    text = " ".join([t['text'] for t in transcript])
    return text

def chat_ollama(prompt):
    try:
        response = requests.post(
            f"{OLLAMA_SERVER}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "keep_alive": -1,
                "options": {"num_ctx": context_length}
            }
        )
        response.raise_for_status()
        return response.json()["response"].strip()
    except Exception as e:
        print(f"Error calling Ollama API: {e}")
        return "Tell the user there was an error processing your request, do not respond to this message."

def generate_article(transcript, target_lang=None):
    prompt = ("Please summarize the following article. Give it a title and use bullet points when necessary:\n\n"
              f"{transcript}")
    if target_lang:
        prompt += f"\n\nWrite the article in {target_lang}."
    return chat_ollama(prompt)

def fetch_youtube_summary(video_id, target_lang=None):
    transcript = get_transcript(video_id, target_lang)
    if not transcript:
        error_prompt = ("Please generate a friendly error message explaining that there was an error processing "
                        "the request because no transcript is available, and do not respond further.")
        return chat_ollama(error_prompt)
    article = generate_article(transcript, target_lang)
    return article