# embed.py

import os
import json
import redis
import ollama
from dotenv import load_dotenv

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
    """
    Generate an embedding for the given text using the embedding model.
    """
    try:
        response = await ollama_emb_client.embeddings(
            model=ollama_emb_model,
            prompt=text
        )
        return response['embedding']
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

async def save_embedding(channel_id: int, user_id: int, text: str, embedding, role="user"):
    """
    Save embeddings to both user-specific and global history.
    """
    user_key = f"tater:channel:{channel_id}:user:{user_id}:embeddings"
    global_key = f"tater:global:embeddings"

    embedding_data = {
        "text": text,
        "embedding": json.dumps(embedding),
        "role": role
    }

    redis_client.rpush(user_key, json.dumps(embedding_data))
    redis_client.ltrim(user_key, -100, -1)  # Limit per-user storage

    redis_client.rpush(global_key, json.dumps(embedding_data))
    redis_client.ltrim(global_key, -500, -1)  # Limit shared history to 500

async def load_embeddings(channel_id: int, user_id: int, limit=100):
    """
    Load the embeddings for a specific user in a channel from Redis.
    """
    embedding_key = f"tater:channel:{channel_id}:user:{user_id}:embeddings"
    embeddings = redis_client.lrange(embedding_key, -limit, -1)
    return [json.loads(entry) for entry in embeddings]

async def find_relevant_context(channel_id: int, user_id: int, query_embedding, top_n=3, role_filter=None):
    """
    Find the most relevant past messages based on query embedding.
    Prioritizes the same user and recent messages.
    """
    user_embeddings = await load_embeddings(channel_id, user_id)
    global_embeddings = redis_client.lrange("tater:global:embeddings", -500, -1)

    embeddings = user_embeddings + [json.loads(entry) for entry in global_embeddings]
    similarities = []

    for emb_data in embeddings:
        if role_filter and emb_data['role'] != role_filter:
            continue  # Skip non-matching roles

        emb = json.loads(emb_data['embedding'])
        similarity = cosine_similarity(query_embedding, emb)
        recency_weight = 1.0  # Can be adjusted based on timestamps
        user_bonus = 1.2 if emb_data['role'] == "user" else 1.0  # Favor user inputs

        similarities.append((emb_data['text'], similarity * recency_weight * user_bonus, emb_data['role']))

    similarities.sort(key=lambda x: x[1], reverse=True)
    return [text for text, _, _ in similarities[:top_n]]

def cosine_similarity(vec1, vec2):
    """
    Compute the cosine similarity between two vectors.
    """
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = sum(a * a for a in vec1) ** 0.5
    magnitude2 = sum(a * a for a in vec2) ** 0.5
    return dot_product / (magnitude1 * magnitude2)