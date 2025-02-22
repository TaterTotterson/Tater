<div align="center"> <img src="https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-AI/refs/heads/main/tater.png" alt="Tater Discord Bot" width="200"/> <h1>Tater Discord AI</h1> </div>

# Tater - A Discord Bot Powered by Ollama

A Discord bot that integrates with Ollama to provide a variety of tools to users.

## Features

- **Conversation Continuity**: Maintains context using Redis and an embedding model for improved memory retrieval.
- **Ollama Integration**: Utilizes Ollama for AI responses, conversation memory, and embedding-based recall.
  - **Chat Responses**: Generates AI responses, waiting messages, and friendly error messages.
  - **Embedding Model**: Enhances chat history recall and provides more relevant responses by storing and retrieving past conversations.
  - **Requirements**:
    - You must use an **Ollama model that supports tools**. For more details, see [Ollama Tools](https://ollama.com/search?c=tools).
    - You must use an **Ollama embedding model**. See available models here: [Ollama Embeddings](https://ollama.com/search?c=embedding).

## Embedding System (Memory & Context Retrieval)

The bot uses an **embedding model** to store and retrieve past messages, improving chat continuity and memory recall. Instead of relying solely on chat history, it:
- **Generates an embedding (vector representation) of each message.**
- **Stores embeddings in Redis** for fast retrieval.
- **Finds relevant past messages** when users bring up similar topics.

### **Low RAM Mode (Optional)**
- By default, the bot **stores all embeddings indefinitely**, allowing it to recall long-term conversations.
- If running on a **low-RAM system**, you can enable memory limits by modifying `embed.py`:
  ```python
  # Uncomment the following line in embed.py to limit storage to the last 100 messages (saves RAM)
  # redis_client.ltrim(global_key, -100, -1)
  ```
  - **Uncommenting this line** will ensure only the **last 100 embeddings** are kept in memory.
  - This helps prevent excessive memory usage on systems with limited resources.

## Available Tools

**Below are the tools available to you. Simply ask Tater to perform these tasks—no slash commands or specific key terms are required:**

- **YouTube Video Summaries:**  
  Extracts YouTube video IDs, fetches summaries, and sends formatted responses.

- **Web Summaries:**  
  Summarizes webpages or articles.

- **Image Generation:**  
  Generates images based on text prompts using Automatic111/SD.Next.

- **Premiumize.me Integration:**  
  - Checks if a given URL is cached on Premiumize.me and retrieves download links.  
  - Processes torrent files to extract the torrent hash, checks cache status, and retrieves download links.

- **RSS Feed Monitoring:**  
  Automatically monitors RSS feeds for new articles and announces summaries to RESPONSE_CHANNEL when new articles are published. This integration includes three tools:  
  - **Watch Feed:** Add an RSS feed to be monitored.  
  - **Unwatch Feed:** Remove an RSS feed from monitoring.  
  - **List Feeds:** List all currently watched RSS feeds.

- **Web Search:**  
  Searches the web for additional or up-to-date information when needed. If the AI determines that it lacks sufficient knowledge or context to answer a query, it can trigger a web search to retrieve current information and use it to generate a final, accurate answer.

## Installation

### Prerequisites
- Python 3.11
- Docker (optional, for containerized deployment)
- Poetry (for dependency management)

### Setting Up Locally

1. **Clone the Repository**

```bash
git clone https://github.com/MasterPhooey/Tater.git
```

2. **Navigate to the Project Directory**

```bash
cd tater
```

3. **Install Dependencies**

```bash
poetry install
```

4. **Configure Environment Variables**

Create a `.env` file in the root directory with the following variables:

```bash
DISCORD_TOKEN=your_discord_token
RESPONSE_CHANNEL_ID=your_channel_id
OLLAMA_HOST=127.0.0.1
OLLAMA_PORT=11434
OLLAMA_MODEL=mistral-small:24b
OLLAMA_EMB_MODEL=nomic-embed-text
CONTEXT_LENGTH=10000
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
AUTOMATIC_URL=http://127.0.0.1:7860
PREMIUMIZE_API_KEY=your_premiumize_api_key
ADMIN_USER_ID=1234567891234567891
```

5. **Run the Bot**

```bash
poetry run python main.py
```

### Running with Docker

1. **Build the Docker Image**

```bash
docker build -t tater .
```

2. **Run the Container**

```bash
docker run -d --name tater_bot tater
```
