# embed.py
import os
import json
import redis
import ollama
import logging
from dotenv import load_dotenv

# Set up logging
logger = logging.getLogger("discord.tater")
logger.setLevel(logging.INFO)  # Set to DEBUG for detailed logging
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Load environment variables
load_dotenv()
ollama_host = os.getenv('OLLAMA_HOST', '127.0.0.1')
ollama_port = int(os.getenv('OLLAMA_PORT', 11434))
ollama_emb_model = os.getenv('OLLAMA_EMB_MODEL', 'nomic-embed-text').strip()
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))

# Initialize Redis client
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# Initialize Ollama client for embeddings
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

async def save_embedding(text: str, embedding):
    global_key = "tater:global:embeddings"
    redis_client.rpush(global_key, json.dumps({"text": text, "embedding": json.dumps(embedding)}))
    logger.info("Message saved")  # Now this logs for every saved message

async def find_relevant_context(query_embedding, top_n=3):
    global_embeddings = redis_client.lrange("tater:global:embeddings", 0, -1)
    similarities = []
    for emb_data in global_embeddings:
        emb_data = json.loads(emb_data)
        emb = json.loads(emb_data["embedding"])
        similarity = cosine_similarity(query_embedding, emb)
        similarities.append((emb_data["text"], similarity))
    similarities.sort(key=lambda x: x[1], reverse=True)
    return [text for text, _ in similarities[:top_n]]

def cosine_similarity(vec1, vec2):
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = sum(a * a for a in vec1) ** 0.5
    magnitude2 = sum(a * a for a in vec2) ** 0.5
    return dot_product / (magnitude1 * magnitude2) if magnitude1 and magnitude2 else 0.0