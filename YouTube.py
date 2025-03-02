import os
import json
import requests
import re
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound

# Load environment variables
load_dotenv()
# Use OLLAMA_HOST and OLLAMA_PORT to build the server URL
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_SERVER = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral-small:24b")
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

def extract_video_id(youtube_url):
    """Extract the video ID from a YouTube URL."""
    parsed_url = urlparse(youtube_url)
    if parsed_url.hostname in ['www.youtube.com', 'youtube.com']:
        query_params = parse_qs(parsed_url.query)
        return query_params.get('v', [None])[0]
    elif parsed_url.hostname == 'youtu.be':
        return parsed_url.path.lstrip('/')
    return None

def format_article_for_discord(article):
    """Format the article for Discord by replacing ### with #."""
    return article.replace("### ", "# ")

def split_message(message_content, chunk_size=1500):
    """Split a long message into chunks."""
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
    """
    Fetches the transcript for the given YouTube video ID.
    """
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
    # Combine all text segments into one transcript
    text = " ".join([t['text'] for t in transcript])
    return text

def chat_ollama(prompt):
    """
    Sends a prompt to the Ollama API and returns the generated response.
    """
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
    """
    Generates an article or summary based on the provided transcript using a simplified prompt.
    """
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
    
    # Schedule storing the summary embedding asynchronously.
    if article:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        loop.create_task(_store_summary_embedding(article))
    
    return article

async def _store_summary_embedding(summary):
    embedding = await generate_embedding(summary)
    if embedding:
        await save_embedding(summary, embedding)