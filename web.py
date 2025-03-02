import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import ollama
import asyncio  # <-- added
from embed import generate_embedding, save_embedding  # <-- make sure these are imported

# Load environment variables
load_dotenv()

ollama_host = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
ollama_port = os.getenv("OLLAMA_PORT", "11434").strip()
ollama_url = f"http://{ollama_host}:{ollama_port}"
ollama_model = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

def extract_article_text(webpage_url):
    # ... (existing code)
    try:
        response = requests.get(webpage_url, timeout=10)
        if response.status_code != 200:
            return None
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
            element.decompose()
        lines = [line.strip() for line in soup.get_text(separator="\n").splitlines() if line.strip()]
        article_text = "\n".join(lines)
        return article_text
    except Exception as e:
        print(f"Error extracting article: {e}")
        return None

def fetch_web_summary(webpage_url, model=ollama_model):
    """
    Extracts and summarizes article text from a webpage.
    Also schedules the summary to be embedded and saved.
    """
    article_text = extract_article_text(webpage_url)
    if not article_text:
        return None

    prompt = f"Please summarize the following article. Give it a title and use bullet points when necessary:\n\n{article_text}"
    try:
        client = ollama.Client(host=ollama_url)
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        summary = response['message'].get('content', '')
        
        # Schedule storing the summary embedding asynchronously.
        if article:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
            loop.create_task(_store_summary_embedding(article))
            
        return summary
    except Exception as e:
        print(f"Error fetching web summary: {e}")
        return None

async def _store_summary_embedding(summary):
    embedding = await generate_embedding(summary)
    if embedding:
        await save_embedding(summary, embedding)

def format_summary_for_discord(summary):
    return summary.replace("### ", "# ")

def split_message(message_content, chunk_size=1500):
    # ... (existing code)
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