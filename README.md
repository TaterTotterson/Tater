<div align="center">
  <img src="https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater-animated.webp" alt="Tater Discord Bot" width="200"/>
  <h1>Tater</h1>
</div>

Tater is a AI Assistant that integrates with Ollama to provide a variety of AI-powered tools, Tater has a web UI for setup and internal private chat. Whether you're on Discord, IRC or using the WebUI, Tater is at your service.

The following plugins can be triggerd simple by asking Tater after you enable them, ex: ``` Tater summarize this youtube video http://youtube.com/watch?v=000000 ```


## 🧩 Tater Plugin Overview

| Plugin Name              | Description                                                                 | Platform              |
|--------------------------|-----------------------------------------------------------------------------|------------------------|
| `youtube_summary`        | Summarizes YouTube videos.                                                  | discord, webui, irc    |
| `web_summary`            | Summarizes content from a provided URL                                      | discord, webui, irc    |
| `web_search`             | Performs web search to help answer user questions                           | discord, webui, irc    |
| `emoji_ai_responder`     | Picks a relevant emoji based on a message when someone reacts to it         | discord                |
| `vision_describer`       | Analyzes uploaded images and returns AI-generated descriptions              | discord                |
| `automatic_plugin`       | Generates images using AUTOMATIC1111 API based on user prompt               | discord, webui         |
| `comfyui_plugin`         | Generates images with ComfyUI using custom workflow templates               | discord, webui         |
| `comfyui_image_video`    | Animates images into WebP loops using ComfyUI.                              | discord                |
| `comfyui_video_plugin`   | Creates videos from prompts using ComfyUI and video workflows               | discord, webui         |
| `comfyui_audio_plugin`   | Generates music/audio from prompts using ComfyUI                            | discord, webui         |
| `comfyui_audio_ace`      | Composes full-length songs using AceStep. Generates lyrics, tags, and MP3s  | discord, webui         |
| `ftp_browser`            | Allows users to browse FTP servers via Discord                              | discord                |
| `sftpgo_account`         | Creates SFTPGo user accounts and their credentials                          | discord, irc           |
| `sftpgo_activity`        | Views SFTPGo user activity like file transfers and sessions                 | discord, irc           |
| `premiumize_torrent`     | Checks if a torrent is cached on Premiumize and returns download links      | discord, webui         |
| `premiumize_download`    | Checks Premiumize for cached file links and returns downloads               | discord, webui, irc    |

### 📡 RSS Feed Watcher (Built-in)

This system runs in the background and posts summarized RSS feed updates. The following plugins enhance or interact with this watcher:

| Plugin Name              | Description                                                                 | Type              | Platform         |
|--------------------------|-----------------------------------------------------------------------------|-------------------|------------------|
| `discord_notifier`       | Posts RSS updates directly to a configured Discord channel                  | RSS Notifier      | plugin-triggered  |
| `telegram_notifier`      | Sends RSS updates to a Telegram channel using the internal feed watcher     | RSS Notifier      | plugin-triggered  |
| `wordpress_poster`       | Posts RSS updates to WordPress using the internal feed watcher              | RSS Notifier      | plugin-triggered  |
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

---

## 🔍 Web Search Plugin Setup (Google Custom Search)

Follow these steps to enable Google Custom Search API and connect it to your bot.

### Step 1: Create or Select a Google Cloud Project

1. Go to: https://console.cloud.google.com/projectselector2/home/dashboard
2. Click **"New Project"** or select an existing one.
3. Note your **Project ID** (you’ll need it in later steps).

### Step 2: Enable the Custom Search API

1. Visit this link (replace `your-project-id` if needed):  
   https://console.developers.google.com/apis/api/customsearch.googleapis.com/overview
2. Make sure your project is selected (top navbar).
3. Click **"Enable"**.

### Step 3: Create an API Key

1. Go to: https://console.cloud.google.com/apis/credentials
2. Click **“+ CREATE CREDENTIALS” > API key**
3. Copy the generated API key.

### Step 4: Set Up a Programmable Search Engine (CSE)

1. Go to: https://programmablesearchengine.google.com/controlpanel/create
2. In **Sites to search**, enter: `*.com` or just `www.google.com` temporarily.
3. Click **Create**.
4. Go to **Control Panel > Basics**, find your **Search engine ID** (CX).
5. Click **“Search the entire web”** under **Sites to search**.
6. Save your changes.

### Step 5: Add Keys to the Plugin Settings

In your Tater bot WebUI:
1. Open the **Plugin Settings** sidebar.
2. Find the **Web Search** plugin.
3. Paste:
   - **Google API Key** → from Step 3
   - **Google Search Engine ID (CX)** → from Step 4
4. Save the settings.

### ✅ Done! Test a search in Discord, WebUI, or IRC:

---

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
