<div align="center">
  <img src="https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater-animated.webp" alt="Tater Discord Bot" width="200"/>
  <h1>Tater</h1>
</div>

**Tater** is an AI assistant that connects to any OpenAI API–compatible LLM, giving you a powerful set of AI-driven tools. It includes a WebUI for setup and private chats, and works across **Discord**, **IRC**, and the WebUI itself.  
**Note:** If you’re using anything other than Ollama, make sure you have a [correct prompt template configured](https://github.com/MasterPhooey/Tater/wiki/Prompt-Templates) for your app to get the best results.

---

## 🌐 Tater Platform Overview

| Platform   | Description                                                                     |
|------------|---------------------------------------------------------------------------------|
| `webui`    | Streamlit-based WebUI for chatting, configuring plugins, and managing settings. |
| `discord`  | Full-featured discord bot that runs compatible plugins                          |
| `irc`      | Lightweight IRC bot that responds to mentions and runs compatible plugins.      |

## 🧩 Tater Plugin Overview

The following plugins can be triggerd simple by asking Tater after you enable them, ex: ``` Tater summarize this youtube video http://youtube.com/watch?v=000000 ```

| Plugin Name                   | Description                                                                 | Platform              |
|------------------------------|-----------------------------------------------------------------------------|-----------------------|
| `youtube_summary`            | Summarizes YouTube videos.                                                  | discord, webui, irc   |
| `web_summary`                | Summarizes content from a provided URL                                      | discord, webui, irc   |
| `web_search`                 | Performs web search to help answer user questions                           | discord, webui, irc   |
| `emoji_ai_responder`         | Picks a relevant emoji based on a message when someone reacts to it         | discord               |
| `vision_describer`           | Analyzes uploaded images and returns AI-generated descriptions              | discord, webui        |
| `automatic_plugin`           | Generates images using AUTOMATIC1111 API based on user prompt               | discord, webui        |
| `comfyui_image_plugin`       | Generates images with ComfyUI using custom workflow templates               | discord, webui        |
| `comfyui_image_video`        | Animates images into WebP loops using ComfyUI.                              | webui                 |
| `comfyui_video_plugin`       | Creates videos from prompts using ComfyUI and video workflows               | webui                 |
| `comfyui_audio_ace`          | Composes full-length songs using AceStep. Generates lyrics, tags, and MP3s  | discord, webui        |
| `comfyui_music_video_plugin` | Generates complete AI music videos with lyrics, audio, and visuals          | webui                 |
| `ftp_browser`                | Allows users to browse FTP servers via Discord                              | discord               |
| `webdav_browser`             | Allows browsing and downloading files from WebDAV servers                   | discord               |
| `sftpgo_account`             | Creates SFTPGo user accounts and their credentials                          | discord, irc          |
| `sftpgo_activity`            | Views SFTPGo user activity like file transfers and sessions                 | discord, irc          |
| `premiumize_torrent`         | Checks if a torrent is cached on Premiumize and returns download links      | discord        |
| `premiumize_download`        | Checks Premiumize for cached file links and returns downloads               | discord, webui, irc   |

### 📡 RSS Feed Watcher (Built-in)

This system runs in the background and posts summarized RSS feed updates. The following plugins enhance or interact with this watcher:

| Plugin Name              | Description                                                                 | Type              | Platform               |
|--------------------------|-----------------------------------------------------------------------------|-------------------|------------------------|
| `discord_notifier`       | Posts RSS updates directly to a configured Discord channel                  | RSS Notifier      | plugin-triggered       |
| `telegram_notifier`      | Sends RSS updates to a Telegram channel using the internal feed watcher     | RSS Notifier      | plugin-triggered       |
| `wordpress_poster`       | Posts RSS updates to WordPress using the internal feed watcher              | RSS Notifier      | plugin-triggered       |
| `list_feeds`             | Lists all RSS feeds being watched by the internal feed watcher              | RSS Management    | discord, webui, irc    |
| `watch_feed`             | Adds a feed to the internal RSS watcher                                     | RSS Management    | discord, webui, irc    |
| `unwatch_feed`           | Removes a feed from the internal RSS watcher                                | RSS Management    | discord, webui, irc    |

Here are some examples of the RSS watcher in action:
- **WordPress Poster**: [tater.news](https://tater.news)
- **Telegram Notifier**: [@TaterNews](https://t.me/TaterNews)
---
**Note**:
- You don't have to use a model that is tagged with tools, test different models if the one you are using isnt trigging the plugins.
- Tater currently recommends using gemma3:27b

## Installation

### Prerequisites
- Python 3.11
- **Redis-Stack**
- LLM
- Docker (optional, for containerized deployment)

### Setting Up Locally

1. **Clone the Repository**

```bash
git clone https://github.com/MasterPhooey/Tater.git
```

2. **Navigate to the Project Directory**

```bash
cd Tater
```

3. **Install Dependencies**

Using pip, run:

```bash
pip install -r requirements.txt
```

4. **Configure Environment Variables**

Create a `.env` file in the root directory with the following variables:

```bash
LLM_HOST=127.0.0.1
LLM_PORT=11434
LLM_MODEL=gemma3:27b
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
  -e LLM_HOST=127.0.0.1 \
  -e LLM_PORT=11434 \
  -e LLM_MODEL=gemma3:27b \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  masterphooey/tater-webui
```

### 3. Access the Web UI

Once the container is running, open your browser and navigate to:

[http://localhost:8501](http://localhost:8501)

The Streamlit-based web UI will be available for interacting with Tater.

---

https://github.com/user-attachments/assets/dffa38fb-b4a5-45e8-a98a-614263446040

