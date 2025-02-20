import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import ollama  # Ensure you have the correct version of the ollama library installed

# Load environment variables
load_dotenv()

# Read the Ollama configuration from the environment.
ollama_host = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
ollama_port = os.getenv("OLLAMA_PORT", "11434").strip()
ollama_url = f"http://{ollama_host}:{ollama_port}"  # Custom endpoint for Ollama
ollama_model = os.getenv("OLLAMA_MODEL", "llama3.2").strip()  # e.g., "mistral-small:24b"
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

def extract_article_text(webpage_url):
    """
    Extract the main textual content from a webpage URL.
    Uses requests and BeautifulSoup to retrieve and parse the HTML.
    """
    try:
        response = requests.get(webpage_url, timeout=10)
        if response.status_code != 200:
            return None
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        # Remove common unwanted elements.
        for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
            element.decompose()
        # Extract text with newline separators.
        text = soup.get_text(separator="\n")
        # Clean up the text: remove extra whitespace and blank lines.
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        article_text = "\n".join(lines)
        return article_text
    except Exception as e:
        print(f"Error extracting article: {e}")
        return None

def fetch_web_summary(webpage_url, model=ollama_model):
    """
    Extract the article text from a webpage and summarize it using the Ollama model.
    This function is synchronous.
    """
    article_text = extract_article_text(webpage_url)
    if not article_text:
        return None

    # Construct a summarization prompt.
    prompt = f"Please summarize the following article. Give it a title and use bullet points when necessary:\n\n{article_text}"

    try:
        # Create an Ollama client using the custom host.
        client = ollama.Client(host=ollama_url)
        # Call chat() with the model specified.
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        summary = response['message'].get('content', '')
        return summary
    except Exception as e:
        print(f"Error fetching web summary: {e}")
        return None

def format_summary_for_discord(summary):
    """
    Optionally format the summary text for Discord.
    """
    return summary.replace("### ", "# ")

def split_message(message_content, chunk_size=1500):
    """
    Split a long message into chunks so it can be sent as multiple Discord messages.
    """
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