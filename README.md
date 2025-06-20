<div align="center">
  <img src="https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater-animated.webp" alt="Tater Discord Bot" width="200"/>
  <h1>Tater - A Ollama Discord Bot & Web UI with Plugins</h1>
</div>

Tater is a Discord bot that integrates with Ollama to provide a variety of AI-powered tools, Tater has a web UI for setup and internal private chat. Whether you're on Discord or using the WebUI, Tater is at your service.

The following plugins can be triggerd simple by asking Tater after you enable them, ex: ``` Tater summarize this youtube video http://youtube.com/watch?v=000000 ```


## 🧩 Tater Plugin Overview

| Plugin Name              | Description                                                                 | Type              | Platform         |
|--------------------------|-----------------------------------------------------------------------------|-------------------|------------------|
| `youtube_summary`        | Summarizes YouTube videos using transcript API (with `yt-dlp` fallback)     | AI Summarization  | discord, webui    |
| `web_summary`            | Summarizes content from a provided URL                                      | AI Summarization  | discord, webui    |
| `web_search`             | Performs web search and returns summarized results                          | AI Search         | discord, webui    |
| `emoji_ai_responder`     | Picks a relevant emoji based on a message when someone reacts to it         | AI Utility        | discord           |
| `vision_describer`       | Analyzes uploaded images and returns AI-generated descriptions              | AI Vision         | discord           |
| `automatic_plugin`       | Generates images using AUTOMATIC1111 API based on user prompt               | Image Generation  | discord, webui    |
| `comfyui_plugin`         | Generates images with ComfyUI using custom workflow templates               | Image Generation  | discord, webui    |
| `comfyui_image_video`    | Animates images into WebP loops using ComfyUI + WanImageToVideo             | Animation         | discord           |
| `comfyui_video_plugin`   | Creates videos from prompts using ComfyUI and video workflows               | Video Generation  | discord, webui    |
| `comfyui_audio_plugin`   | Generates music/audio from prompts using ComfyUI                            | Audio Generation  | discord, webui    |
| `ftp_browser`            | Allows users to browse FTP servers via Discord                              | File Access       | discord           |
| `sftpgo_account`         | Lists and manages SFTPGo user accounts and their credentials                | File Access       | discord           |
| `sftpgo_activity`        | Views SFTPGo user activity like file transfers and sessions                 | File Access       | discord           |
| `premiumize_torrent`     | Checks if a torrent is cached on Premiumize and returns download links      | Cloud Utility     | discord, webui    |
| `premiumize_download`    | Checks Premiumize for cached file links and returns downloads               | Cloud Utility     | discord, webui    |

### 📡 RSS Feed Watcher (Built-in)

This system runs in the background and posts RSS feed updates. The following plugins enhance or interact with this watcher:

| Plugin Name              | Description                                                                 | Type              | Platform         |
|--------------------------|-----------------------------------------------------------------------------|-------------------|------------------|
| `discord_notifier`       | Posts RSS updates directly to a configured Discord channel                  | RSS Notifier      | plugin-triggered  |
| `telegram_notifier`      | Sends RSS updates to a Telegram channel using the internal feed watcher     | RSS Notifier      | plugin-triggered  |
| `wordpress_poster`       | Posts RSS updates to WordPress using the internal feed watcher              | RSS Publisher     | plugin-triggered  |
| `list_feeds`             | Lists all RSS feeds being watched by the internal feed watcher              | RSS Management    | discord, webui    |
| `watch_feed`             | Adds a feed to the internal RSS watcher                                     | RSS Management    | discord, webui    |
| `unwatch_feed`           | Removes a feed from the internal RSS watcher                                | RSS Management    | discord, webui    |


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
NOTE: this should be enabled by default in Redis-Stack

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
