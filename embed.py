# embed.py
import os
import json
import redis
import ollama
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("discord.tater")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

ollama_host = os.getenv('OLLAMA_HOST', '127.0.0.1')
ollama_port = int(os.getenv('OLLAMA_PORT', 11434))
ollama_emb_model = os.getenv('OLLAMA_EMB_MODEL', 'nomic-embed-text').strip()
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))

redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
ollama_emb_client = ollama.AsyncClient(host=f'http://{ollama_host}:{ollama_port}')

async def generate_embedding(text: str):
    try:
        response = await ollama_emb_client.embeddings(
            model=ollama_emb_model,
            prompt=text,
            keep_alive=-1
        )
        return response['embedding']
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        return None

async def save_embedding(text: str, embedding, username: str):
    """
    Save the embedding along with the text and the username.
    """
    global_key = "tater:global:embeddings"
    # Store a JSON object with username, text, and embedding
    redis_client.rpush(global_key, json.dumps({
        "username": username,
        "text": text,
        "embedding": json.dumps(embedding)
    }))
    # Uncomment the following line to limit storage to the last 1000 messages (saves RAM)
    # redis_client.ltrim(global_key, -1000, -1)
    logger.info("Message saved")

async def find_relevant_context(query_embedding, top_n=10):
    # Guard clause: if query_embedding is None, return an empty list.
    if query_embedding is None:
        return []
    global_embeddings = redis_client.lrange("tater:global:embeddings", 0, -1)
    similarities = []
    for emb_data in global_embeddings:
        try:
            data = json.loads(emb_data)
            stored_emb_str = data.get("embedding")
            if stored_emb_str is None:
                continue  # Skip if no embedding is stored.
            emb = json.loads(stored_emb_str)
            if emb is None:
                continue  # Skip if decoding returns None.
            similarity = cosine_similarity(query_embedding, emb)
            # Include username with the text for context.
            combined_text = f"{data.get('username', '')}: {data['text']}"
            similarities.append((combined_text, similarity))
        except Exception as e:
            logger.error(f"Error processing embedding: {e}")
            continue
    similarities.sort(key=lambda x: x[1], reverse=True)
    return [text for text, _ in similarities[:top_n]]

def cosine_similarity(vec1, vec2):
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = sum(a * a for a in vec1) ** 0.5
    magnitude2 = sum(a * a for a in vec2) ** 0.5
    return dot_product / (magnitude1 * magnitude2) if magnitude1 and magnitude2 else 0.0