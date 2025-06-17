<div align="center">
  <img src="https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater-animated.webp" alt="Tater Discord Bot" width="200"/>
  <h1>Tater - A Ollama Discord Bot & Web UI with Plugins</h1>
</div>

Tater is a Discord bot that integrates with Ollama to provide a variety of AI-powered tools, Tater has a web UI for setup and internal private chat. Whether you're on Discord or using the WebUI, Tater is at your service.

## 🧩 Tater Plugin Overview

| Plugin Name              | Description                                                                 | Type              | Trigger         |
|--------------------------|-----------------------------------------------------------------------------|-------------------|------------------|
| `youtube_summary`        | Summarizes YouTube videos using transcript API (with `yt-dlp` fallback)     | AI Summarization  | AI-triggered     |
| `web_summary`            | Summarizes content from a provided URL                                      | AI Summarization  | AI-triggered     |
| `web_search`             | Performs web search and returns summarized results                          | AI Search         | AI-triggered     |
| `emoji_ai_responder`     | Picks a relevant emoji based on a message when someone reacts to it         | AI Utility        | Event-triggered  |
| `vision_describer`       | Analyzes uploaded images and returns AI-generated descriptions              | AI Vision         | AI-triggered     |
| `automatic_plugin`       | Generates images using AUTOMATIC1111 API based on user prompt               | Image Generation  | AI-triggered     |
| `comfyui_plugin`         | Generates images with ComfyUI using custom workflow templates               | Image Generation  | AI-triggered     |
| `comfyui_image_video`    | Animates images into WebP loops using ComfyUI + WanImageToVideo             | Animation         | AI-triggered     |
| `comfyui_video_plugin`   | Creates videos from prompts using ComfyUI and video workflows               | Video Generation  | AI-triggered     |
| `comfyui_audio_plugin`   | Generates music/audio from prompts using ComfyUI                            | Audio Generation  | AI-triggered     |
| `ftp_browser`            | Allows users to browse FTP servers via Discord                              | File Access       | AI-triggered     |
| `sftpgo_account`         | Lists and manages SFTPGo user accounts and their credentials                | File Access       | AI-triggered     |
| `sftpgo_activity`        | Views SFTPGo user activity like file transfers and sessions                 | File Access       | AI-triggered     |
| `premiumize_torrent`     | Checks if a torrent is cached on Premiumize and returns download links      | Cloud Utility     | AI-triggered     |
| `premiumize_download`    | Checks Premiumize for cached file links and returns downloads               | Cloud Utility     | AI-triggered     |
| `telegram_notifier`      | Posts formatted messages to a Telegram channel                              | Notifier          | Plugin-triggered |
| `wordpress_poster`       | Posts summaries/announcements to WordPress via REST API                     | Publisher         | Plugin-triggered |
| `list_feeds`             | Lists all RSS feeds being monitored                                         | RSS Management    | Manual/AI        |
| `watch_feed`             | Adds a feed to the RSS watchlist                                            | RSS Management    | Manual           |
| `unwatch_feed`           | Removes a feed from the watchlist                                           | RSS Management    | Manual           |

**Note**:
- You don't have to use a model that is tagged with tools, test different models if the one you are using isnt trigging the plugins.
- Tater currently recommends using gemma3:27b

## Installation

### Prerequisites
- Python 3.11
- **Redis-Stack**
- Ollama
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
CONTEXT_LENGTH=5000
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```

5. **Run the Web UI**

Launch the web UI using Streamlit:

```bash
streamlit run webui.py
```

## Docker

### 1. Pull the Image

Pull the prebuilt image with the following command:

```bash
docker pull masterphooey/tater-webui
```

### 2. Configuring Environment Variables

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

### 3. Access the Web UI

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
