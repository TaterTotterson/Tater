<div align="center">
  <img src="https://raw.githubusercontent.com/TaterTotterson/Tater/refs/heads/main/images/tater-animated.webp" alt="Tater Discord Bot" width="200"/>
  <h1>Tater</h1>
</div>

**Tater** is an AI assistant that connects to any OpenAI-compatible LLM, giving you a powerful set of AI-driven tools. It includes a WebUI for setup and private chats, and works across **Discord**, **IRC**, **Matrix**, **Home Assistant**, **HomeKit**, and even the **OG Xbox via XBMC4Xbox**

---

## 🌐 Tater Platform Overview

| Platform          | Description |
|-------------------|-------------|
| `discord`         | Full-featured Discord bot that runs compatible plugins and supports rich interactions, media output, and background jobs. |
| `homeassistant`   | Native Home Assistant integration via the Assist pipeline, allowing Tater to act as a voice-enabled AI assistant and control devices directly. <br>🔗 https://github.com/TaterTotterson/Tater-HomeAssistant |
| `ha_automations`  | Lightweight Home Assistant **automation-only** endpoint for direct tool execution (no AI routing). Designed for fast, reliable automations like camera events, doorbell alerts, weather summaries, and dashboard sensors. Intended to be used via the **Tater Automations** Home Assistant custom component, which provides a native “Call Tater automation tool” action. <br>🔗 https://github.com/TaterTotterson/tater_automations |
| `homekit`         | Siri / Apple Shortcuts integration for HomeKit, enabling “Hey Siri, ask Tater…” voice commands, per-device conversation sessions, and plugin-backed actions. <br>🔗 https://github.com/TaterTotterson/Tater/wiki/How-to-Build-the-%E2%80%9CTater%E2%80%9D-Shortcut |
| `irc`             | Lightweight IRC bot that responds to mentions and runs compatible plugins with minimal overhead. |
| `matrix`          | Modern Matrix client with end-to-end encryption support, Markdown rendering, and full plugin compatibility for federated chat networks like Element and Cinny. |
| `xbmc`            | OG Xbox integration for **XBMC4Xbox**, provided by a custom Cortana-powered skin and script, enabling an on-console AI assistant with chat history and plugin-powered actions. <br>🔗 https://github.com/TaterTotterson/skin.cortana.tater-xbmc |
| `webui`           | Streamlit-based WebUI for chatting with Tater, configuring plugins, managing platforms, and monitoring automation activity. |

## 🧩 Tater Plugin Overview

The following plugins can be triggerd simple by asking Tater after you enable them, ex: ``` Tater summarize this youtube video http://youtube.com/watch?v=000000 ```

| Plugin Name                   | Description                                                                 | Platform                                   |
|-------------------------------|-----------------------------------------------------------------------------|--------------------------------------------|
| `automatic_plugin`            | Generates images using AUTOMATIC1111 API based on user prompt               | discord, webui                             |
| `broadcast`                   | Sends a whole-house spoken announcement using Home Assistant TTS (Piper, Cloud, etc.) to configured media_player devices. Triggered by phrases like “broadcast”, “announce”, or “page the house”. | webui, homeassistant, homekit, xbmc        |
| `camera_event`                | Detects motion, describes it with Vision AI, and logs structured events with cooldown. | automations                                |
| `comfyui_audio_ace`           | Composes full-length songs using AceStep. Generates lyrics, tags, and MP3s  | discord, webui, homeassistant, matrix      |
| `comfyui_image_plugin`        | Generates images with ComfyUI using custom workflow templates               | discord, webui, matrix                     |
| `comfyui_image_video`         | Animates images into WebP loops using ComfyUI.                              | webui                                      |
| `comfyui_music_video_plugin`  | Generates complete AI music videos with lyrics, audio, and visuals          | webui                                      |
| `comfyui_video_plugin`        | Creates videos from prompts using ComfyUI and video workflows               | webui                                      |
| `device_compare`              | Compares two devices, fetching specs and FPS benchmarks from online sources | discord, webui, matrix                     |
| `doorbell_alert`              | Triggers when the doorbell rings — captures a snapshot from a Home Assistant camera, analyzes it with Vision AI, announces who or what is at the door via Piper TTS, and logs events to the Automations Platform. | automations                                |
| `emoji_ai_responder`          | Picks a relevant emoji based on a message when someone reacts to it         | discord                                   |
| `events_query`                | Summarizes all stored events by time, area, or activity.                    | webui, homeassistant, homekit              |
| `events_query_brief`          | Automation-focused event summary plugin. Returns a very short, sensor-safe summary of recent household events by area and timeframe. | automations                                |
| `find_my_phone`               | Rings your phone using the Home Assistant Companion App notify service to help locate a misplaced device. Sends high-priority alerts that can repeat until you find it. | webui, homeassistant, homekit, xbmc        |
| `ftp_browser`                 | Allows users to browse FTP servers via Discord                              | discord                                   |
| `ha_control`                  | Controls Home Assistant devices via domain, service, entity, or area (e.g., turn lights on, toggle switches, set temperatures). | webui, homeassistant, homekit, xbmc        |
| `lowfi_video`                 | Generates lofi music videos, outputs 20-min MP4                             | webui                                      |
| `mister_remote`               | Controls MiSTer FPGA via MiSTer Remote API — play, menu, now-playing, and screenshots. Natural language parsing and per-platform output (Discord uploads, WebUI inline, Matrix embedded, voice-safe for HA). | discord, webui, irc, homeassistant, matrix, homekit |
| `obsidian_note`               | Creates new notes in your Obsidian vault with AI-generated titles and content | webui                                      |
| `obsidian_search`             | Searches your entire Obsidian vault and extracts relevant notes to answer questions | webui                                 |
| `overseerr_request`           | Adds a movie or TV show to Overseerr by title, creating a new request for it. Example: add the movie F1, request the TV show One Piece. | webui, homeassistant, homekit              |
| `overseerr_trending`          | Fetches Trending or Upcoming movies/TV shows from Overseerr. Example: what movies are trending, what TV shows are upcoming. | discord, webui, irc, matrix, homekit       |
| `premiumize_download`         | Checks Premiumize for cached file links and returns downloads               | discord, webui, irc, matrix                |
| `premiumize_torrent`          | Checks if a torrent is cached on Premiumize and returns download links      | discord                                   |
| `sftpgo_account`              | Creates SFTPGo user accounts and their credentials                          | discord, webui, irc, matrix                |
| `sftpgo_activity`             | Views SFTPGo user activity like file transfers and sessions                 | discord, webui, irc, matrix                |
| `tater_gits_add_feed`         | Adds a GitHub releases feed to the Tater Gits watcher with auto category    | discord, webui, irc                        |
| `vision_describer`            | Analyzes uploaded images and returns AI-generated descriptions              | discord, webui, matrix                     |
| `voicepe_remote_timer`        | Starts a device-local timer on a Voice PE using ESPHome-exposed entities (seconds + start/cancel). Requires updated Voice PE config: [voicePE-TaterTimer.yaml](https://github.com/TaterTotterson/microWakeWords/blob/main/voicePE-TaterTimer.yaml) | webui, homeassistant, homekit, xbmc |
| `weather_query_brief`         | Automation-focused weather summary plugin. Reads recent Home Assistant weather sensors (temperature, wind, rain, etc.) and returns a short, dashboard-safe summary. | automations                                |
| `web_search`                  | Performs web search to help answer user questions                           | discord, webui, irc, homeassistant, matrix, homekit, xbmc |
| `web_summary`                 | Summarizes content from a provided URL                                      | discord, webui, irc, matrix                |
| `webdav_browser`              | Allows browsing and downloading files from WebDAV servers                   | discord                                   |
| `youtube_summary`             | Summarizes YouTube videos                                                   | discord, webui, irc, matrix                |

### 📡 RSS Feed Watcher (Built-in)

This system runs in the background and posts summarized RSS feed updates. The following plugins enhance or interact with this watcher:

| Plugin Name              | Description                                                                 | Type              | Platform               |
|--------------------------|-----------------------------------------------------------------------------|-------------------|------------------------|
| `discord_notifier`       | Posts RSS updates directly to a configured Discord channel                  | RSS Notifier      | plugin-triggered       |
| `telegram_notifier`      | Sends RSS updates to a Telegram channel using the internal feed watcher     | RSS Notifier      | plugin-triggered       |
| `wordpress_poster`       | Posts RSS updates to WordPress using the internal feed watcher              | RSS Notifier      | plugin-triggered       |
| `ntfy_notifier`          | Sends RSS updates to an ntfy topic for instant push notifications           | RSS Notifier      | plugin-triggered       |
| `list_feeds`             | Lists all RSS feeds being watched by the internal feed watcher              | RSS Management    | discord, webui, irc    |
| `watch_feed`             | Adds a feed to the internal RSS watcher                                     | RSS Management    | discord, webui, irc    |
| `unwatch_feed`           | Removes a feed from the internal RSS watcher                                | RSS Management    | discord, webui, irc    |

Here are some examples of the RSS watcher in action:
- **WordPress Poster**: [TaterByets.com](https://TaterBytes.com)
- **WordPress Poster**: [ThePotatoConsole.com](https://ThePotatoConsole.com)
- **WordPress Poster**: [TaterNews.com](https://TaterNews.com)
---
**Note**:
- Do not use a thinking model with tater
- Tater currently recommends using qwen3-next-80b, qwen3-coder-30b or Gemma3-27b

## Installation

### 🏠 Home Assistant (User Friendly)

The easiest way to run **Tater** is as a Home Assistant add-on.  
This method requires no manual Docker setup and integrates cleanly with Home Assistant,
including networking, persistence, and add-on management.

A dedicated Home Assistant add-on repository is available here:

https://github.com/TaterTotterson/hassio-addons-tater

#### Add the Tater add-on repository

Click the button below to add the repository to Home Assistant:

[![Add Repository to Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](
https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https://github.com/TaterTotterson/hassio-addons-tater
)

Once added, the following add-ons will appear in the Home Assistant Add-on Store:

- **Redis Stack** – required for Tater memory, plugins, and automations
- **Tater AI Assistant** – the main Tater service

#### Install order

1. Install and **start Redis Stack**
2. Install **Tater AI Assistant**
3. Configure your LLM and Redis settings in the Tater add-on
4. Start Tater

This is the recommended setup for most users and provides the smoothest experience.

---

### Local Installation (Advanced)

### Prerequisites
- Python 3.11
- **[Redis-Stack](https://hub.docker.com/r/redis/redis-stack)**
- OpenAI API–compatible LLM app (such as **Ollama**, **LocalAI**, **LM Studio**, **Lemonade**, or **OpenAI API**)
- Docker (optional, for containerized deployment)

### Setting Up Locally

1. **Clone the Repository**

```bash
git clone https://github.com/TaterTotterson/Tater.git
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

Create a `.env` file in the root directory.  
Below are example configurations for local LLM backends (Ollama, LM Studio, LocalAI) and ChatGPT (GPT-4o, etc.).

---

Example: Local backend (Ollama, LM Studio, LocalAI)
```
LLM_HOST=127.0.0.1  
LLM_PORT=11434  
LLM_MODEL=gemma3-27b-abliterated  
REDIS_HOST=127.0.0.1  
REDIS_PORT=6379  
```
---

Example: ChatGPT (GPT-4o, etc.)
```
LLM_HOST=https://api.openai.com  
LLM_PORT=  
LLM_MODEL=gpt-4o  
LLM_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx  
REDIS_HOST=127.0.0.1  
REDIS_PORT=6379  
```
Note: When using ChatGPT, leave LLM_PORT blank.  
Tater will automatically connect using HTTPS without appending a port number.

5. **Run the Web UI**

Launch the web UI using Streamlit:

```bash
streamlit run webui.py
```

## Docker

### 1. Pull the Image

Pull the prebuilt image with the following command:

```bash
docker pull ghcr.io/tatertotterson/tater:latest
```

### 2. Configuring Environment Variables

Ensure you supply the required environment variables. You can pass these using the `-e` flag when starting the container.

---

Example: Local backend (Ollama, LM Studio, LocalAI)
```
docker run -d --name tater_webui \
  -p 8501:8501 \
  -p 8787:8787 \
  -p 8788:8788 \
  -p 8789:8789 \
  -p 8790:8790 \
  -e LLM_HOST=127.0.0.1 \
  -e LLM_PORT=11434 \
  -e LLM_MODEL=gemma3-27b-abliterated \
  -e REDIS_HOST=127.0.0.1 \
  -e REDIS_PORT=6379 \
  ghcr.io/tatertotterson/tater:latest
```
---

Example: ChatGPT (GPT-4o, etc.)
```
docker run -d --name tater_webui \
  -p 8501:8501 \
  -p 8787:8787 \
  -p 8788:8788 \
  -p 8789:8789 \
  -p 8790:8790 \
  -e LLM_HOST=https://api.openai.com \
  -e LLM_PORT= \
  -e LLM_MODEL=gpt-4o \
  -e LLM_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx \
  -e REDIS_HOST=127.0.0.1 \
  -e REDIS_PORT=6379 \
  ghcr.io/tatertotterson/tater:latest
```
Note: When using ChatGPT, leave LLM_PORT blank.  
Tater will automatically connect using HTTPS without appending a port number.

### 3. Access the Web UI

Once the container is running, open your browser and navigate to:

[http://localhost:8501](http://localhost:8501)

The Streamlit-based web UI will be available for interacting with Tater.

---


https://github.com/user-attachments/assets/9138f485-ccd6-46e0-9295-f5617c079fea

