<div align="center">
  <img src="https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png" alt="Tater Discord Bot" width="200"/>
  <h1>Tater Discord AI & Web UI</h1>
</div>

# Tater - A Discord Bot & Web UI Powered by Ollama

Tater is a Discord bot that integrates with Ollama to provide a variety of AI-powered tools, and now it also comes with a web UI for internal private chat. Whether you're on Discord or using the WebUI, Tater is at your service.

<div align="center">
  <img src="https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/webui.png" alt="Tater Discord Bot" width="600"/>
  <h1>Tater Discord AI & Web UI</h1>
</div>


## Features

- **Conversation Continuity**: Maintains context using Redis.
- **Ollama Integration**: Utilizes Ollama for AI responses.
  - **Chat Responses**: Generates AI responses, waiting messages, and friendly error messages.
  - **Note**:
    - You DO NOT need to use a model that is tagged with tools to use this, test different models if the one you are using isnt trigging the plugins, but it does not need to be under Ollama model with tools support list.
    - Tater currently recommends using gemma3:27b

    
## Available Tools (Plugins)

**Below are the tools available to you. Simply ask Tater to perform these tasks—no slash commands or specific key terms are required:**

- **YouTube Video Summaries:**  
  Extracts YouTube video IDs, fetches summaries, and sends formatted responses.

**Web Summaries:**  
Summarizes webpages or articles.

**Image Generation:**  
Generates images based on text prompts using Automatic111/ComfyUI.

**Audio Generation:**  
Generates custom audio masterpieces based on your prompts using our new Audio Plugin—enjoy exclusive tunes directly in Discord or via the web interface.

**Vision Plugin:**  
Processes visual inputs to generate descriptive insights or creative interpretations from images.

**Premiumize.me Integration:**  
- Checks if a given URL is cached on Premiumize.me and retrieves download links.  
- Processes torrent files to extract the torrent hash, checks cache status, and retrieves download links.

**RSS Feed Monitoring:**  
Automatically monitors RSS feeds for new articles and announces summaries to RESPONSE_CHANNEL when new articles are published. This integration includes three tools:  
- **Watch Feed:** Add an RSS feed to be monitored.  
- **Unwatch Feed:** Remove an RSS feed from monitoring.  
- **List Feeds:** List all currently watched RSS feeds.

**SFTPgo Account Creation & Status Plugin:**  
Creates accounts an Provides real-time status updates and file transfer statistics from your SFTPgo server.

**Web Search:**  
Searches the web for additional or up-to-date information when needed. If the AI determines that it lacks sufficient knowledge or context to answer a query, it can trigger a web search to retrieve current information and generate a final, accurate answer.


## Plugins:  

For a detailed example on how to create your own plugin, check out the [plugin template](https://github.com/MasterPhooey/Tater-Discord-WebUI/blob/main/examples/plugin-template.py).
  
## Web UI Integration

The web UI provides a Streamlit-based interface to interact with Tater. Key features include:

- **Interactive Chat:**  
  Engage with Tater via an intuitive chat interface that supports chat history, file attachments, and tool function calls.

- **Discord Bot Settings:**  
  Configure Discord-related settings directly from the UI. In the sidebar, under the "Discord Settings" expander, you can set:
  - **DISCORD_TOKEN**: Your bot's token.
  - **ADMIN_USER_ID**: The Discord ID of the admin.
  - **RESPONSE_CHANNEL_ID**: The channel ID where Tater sends responses.
  - **RSS_CHANNEL_ID**: The channel ID for RSS feed announcements.

- **Chat Settings:**  
  Customize your user avatar and username. These settings allow you to personalize your experience and are stored persistently in Redis.

- **File Attachments:**  
  Upload files or images directly from the sidebar, which are then processed accordingly by Tater (for example, torrent files for Premiumize functions).

This unified interface lets you manage both chat interactions and Discord settings in one place, making it easy to deploy and maintain Tater across both platforms.

- **RSS Feed Management** (Discord-only):
  - **Watch Feeds**: Add an RSS feed to the watch list.
  - **Unwatch Feeds**: Remove an RSS feed.
  - **List Feeds**: List all currently watched RSS feeds.
  - (RSS feed announcements post to a dedicated Discord channel.)

## Installation

### Prerequisites
- Python 3.11
- Docker (optional, for containerized deployment)

### Setting Up Locally

1. **Clone the Repository**

```bash
git clone https://github.com/MasterPhooey/Tater-Discord-WebUI.git
```

2. **Navigate to the Project Directory**

```bash
cd Tater-Discord-WebUI
```

3. **Install Dependencies**

Using pip, run:

```bash
pip install -r requirements.txt
```

4. **Configure Environment Variables**

Create a `.env` file in the root directory with the following variables:

```bash
OLLAMA_HOST=127.0.0.1
OLLAMA_PORT=11434
OLLAMA_MODEL=gemma3:27b
CONTEXT_LENGTH=10000
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```

5. **Run the Web UI**

Launch the web UI using Streamlit:

```bash
streamlit run webui.py
```

## Using the Prebuilt Docker Image

For users who prefer not to build the Docker image locally, a prebuilt image is available on Docker Hub.

### 1. Pull the Image

Pull the prebuilt image with the following command:

```bash
docker pull masterphooey/tater-webui
```

### 2. Run the Container

Run the container with the following command. This example maps port 8501 on your host to port 8501 in the container (where the Streamlit web UI runs):

```bash
docker run -d --name tater_webui -p 8501:8501 masterphooey/tater-webui
```

### 3. Configuring Environment Variables

Ensure you supply the required environment variables. You can pass these using the `-e` flag when starting the container. For example:

```bash
docker run -d --name tater_webui \
  -p 8501:8501 \
  -e OLLAMA_HOST=127.0.0.1 \
  -e OLLAMA_PORT=11434 \
  -e OLLAMA_MODEL=gemma3:27b \
  -e CONTEXT_LENGTH=10000 \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  masterphooey/tater-webui
```

### 4. Access the Web UI

Once the container is running, open your browser and navigate to:

[http://localhost:8501](http://localhost:8501)

The Streamlit-based web UI will be available for interacting with Tater.



## Redis Persistence

If you're running Redis directly, you can ensure data persistence by configuring Redis's built-in persistence mechanisms. Redis supports two primary methods:

### 1. **RDB Snapshots:**  
   Redis periodically saves snapshots of your dataset to a file (typically named `dump.rdb`). You can control this behavior in your `redis.conf` file using `save` directives. For example:
   ```bash
   # Save a snapshot every 900 seconds if at least 1 key changed.
   save 900 1

   # Save a snapshot every 300 seconds if at least 10 keys changed.
   save 300 10

   # Save a snapshot every 60 seconds if at least 10000 keys changed.
   save 60 10000
   ```
### 2. Append Only File (AOF):
  Redis can log every write operation to an AOF file, which can be replayed to reconstruct the dataset.
  To enable AOF, add the following lines to your redis.conf:

```bash
appendonly yes
appendfilename "appendonly.aof"
```
### Alternatively, you can launch Redis with these persistence settings directly from the terminal (without modifying your redis.conf):
```bash
redis-server --save "900 1" --save "300 10" --save "60 10000" --appendonly yes --appendfilename "appendonly.aof"
```
