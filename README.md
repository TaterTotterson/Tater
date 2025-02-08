<div align="center"> <img src="https://raw.githubusercontent.com/MasterPhooey/Tater/refs/heads/main/tater.png" alt="Tater Discord Bot" width="150"/> <h1>Tater Discord Bot</h1> </div>

A Discord bot built using Discord.py that integrates with Ollama, Redis, Premiumize.me, YouTube, and Web summarization services to provide a variety of tools to users.

## Features

- **Conversation Continuity:** Maintains context using Redis.
- **Ollama Integration:** Uses Ollama to generate AI responses, waiting messages, and friendly error messages.
           Note: You must use an Ollama model that supports tools. For more details, see [Ollama Tools](https://ollama.com/search?c=tools).
- **YouTube Video Summaries:** Extracts YouTube video IDs, fetches summaries, and sends formatted responses.
- **Web Summaries:** Summarizes webpages or articles.
- **Image Generation:** Generates images based on text prompts.
- **Premiumize.me Integration:**
  - Checks if a given URL is cached on Premiumize.me and retrieves download links.
  - Processes torrent files to extract the torrent hash, checks cache status, and retrieves download links.
- **Custom Bot Status:** Displays a status (e.g., "Playing Nintendo") on startup.

## Installation

### Prerequisites
- Python 3.11
- Docker (optional, for containerized deployment)
- Poetry (for dependency management)

### Setting Up Locally

1. **Clone the Repository**

```bash
git clone <repository_url>
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
OLLAMA_HOST=127.0.0.1
OLLAMA_PORT=11434
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
OLLAMA_MODEL=llama3.2
PREMIUMIZE_API_KEY=your_premiumize_api_key
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

## Configuration

- **Logging:**  
  Adjust the logging level in your code by changing the line:
  
  ```bash
  logging.basicConfig(level=logging.DEBUG)
  ```
  
  to a less verbose level (e.g., `logging.INFO`) for production.
  
- **Redis:**  
  Used to store and retrieve conversation history for maintaining context.
  
- **Ollama:**  
  The bot uses Ollama’s API to generate AI responses, waiting messages, and friendly error messages.
  
- **Premiumize.me:**  
  Integrates with Premiumize.me to check for cached files (both URL and torrent-based) and to retrieve download links.

## Usage

When the bot is running, it listens for messages in a designated channel or when it’s mentioned. Based on the JSON response from Ollama, the bot will execute one of the following tools:

- **YouTube Summary:** Request a summary of a YouTube video.
- **Web Summary:** Request a summary of a webpage.
- **Image Generation:** Generate an image from a text prompt.
- **Premiumize Download Check:** Check if a URL is cached on Premiumize.me and, if so, provide download links.
- **Premiumize Torrent Check:** Process an attached torrent file to check if it’s cached on Premiumize.me and provide download links.
