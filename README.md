<div align="center">
  <img src="images/tater-new-logo.png" alt="Tater AI Assistant" width="300"/>
</div>

**Tater** is an AI assistant that connects to any OpenAI-compatible LLM, giving you a powerful set of AI-driven tools. It includes a WebUI for setup and private chats, and works across **Discord**, **Home Assistant**, **HomeKit**, **IRC**, **macOS**, **Matrix**, **Telegram**, and even the **OG Xbox via XBMC4Xbox**

Main website: [taterassistant.com](https://taterassistant.com)

---

## Cerberus
Tater runs on Cerberus — a closed-loop Planner → Doer → Checker architecture designed for reliable tool execution.

---

## Branding Stack

- **Cerberus AI Core**: reasoning and orchestration layer
- **Verba Plugins**: capabilities, actions, and tools
- **Tater Portals**: external communication bridges (Discord, Matrix, IRC, Telegram, WebUI, Home Assistant, macOS, and more)

Architecture flow:
`User -> Portal -> Cerberus AI Core -> Verba Plugin -> Portal -> User`

---

## 🛒 Tater Shop (Unified Catalog)

Tater uses **Tater Shop** as the source of truth for:

- **Verba Plugins**
- **Tater Portals**
- **Tater Cores**

This repository no longer keeps static lists of those modules in the README.
Instead, catalogs, versions, metadata, and update paths are managed in:

👉 **https://github.com/TaterTotterson/Tater_Shop**

From the WebUI, use:

- **Plugin Manager**
- **Portal Manager**
- **Core Manager**

Each manager supports browse/install/update/remove and startup restore for enabled items that are missing on disk.

---

# Installation
> **Note**:
> - Tater currently recommends using qwen3-coder-next, qwen3-next-80b, gpt-oss-120b, qwen3-coder-30b or Gemma3-27b

<img width="100" height="44" alt="unraid_logo_black-339076895" src="https://github.com/user-attachments/assets/87351bed-3321-4a43-924f-fecf2e4e700f" />

Tater is available in the **Unraid Community Apps** store.

You can install both:
- **Tater**
- **Redis Stack**

directly from the Unraid App Store with a one-click template.

Important for Docker/Unraid persistence:
- Add a path mapping for `/app/agent_lab` (container) -> `/mnt/user/appdata/tater/agent_lab` (host example).
- Without this mapping, data in `/agent_lab` (logs/downloads/documents/workspace) can be lost on container rebuilds/updates.

## 🏠 Home Assistant

A dedicated Home Assistant add-on repository is available here:

https://github.com/TaterTotterson/hassio-addons-tater

### Add the Tater add-on repository

Click the button below to add the repository to Home Assistant:

[![Add Repository to Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](
https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https://github.com/TaterTotterson/hassio-addons-tater
)

Once added, the following add-ons will appear in the Home Assistant Add-on Store:

- **Redis Stack** – required for Tater memory, Verba Plugins, and automations
- **Tater AI Assistant** – the main Tater service

#### Install order

1. Install and **start Redis Stack**
2. Install **Tater AI Assistant**
3. Configure your LLM and Redis settings in the Tater add-on
4. Start Tater

This is the recommended setup for most users and provides the smoothest experience.

## 🔌 Home Assistant Integration (Optional Enhancements)

Tater AI supports deeper integration with Home Assistant through a couple of companion repositories. These extend Tater’s usefulness in smart home setups by enabling voice/text conversation control and native automation triggers.

### 📍 Tater-HomeAssistant
https://github.com/TaterTotterson/Tater-HomeAssistant

A Home Assistant **custom integration** that allows Tater to function as a **Conversation Agent** inside Home Assistant’s Assist pipeline. This enables voice or text interactions from Home Assistant to be routed directly to your Tater backend, where Verba Plugins can be executed and contextual responses returned.

Key benefits:
- Use Tater as a native **voice and text assistant** within Home Assistant
- Routes Assist queries directly to your running Tater AI backend
- Supports Verba Plugins that implement `handle_homeassistant(...)`
- Maintains conversation context for more natural, multi-turn interactions

This integration is required if you want Tater to participate directly in Home Assistant conversations or voice control.

### ⚙️ tater_automations
https://github.com/TaterTotterson/tater_automations

A Home Assistant **automation-focused custom component** that exposes Tater’s tools as **native Home Assistant automation actions**. This allows Home Assistant automations to call specific Tater tools directly, without REST calls, scripts, or YAML workarounds.

Key benefits:
- Adds a native **“Call Tater automation tool”** action in Home Assistant automations
- Designed for fast, reliable, automation-only execution
- Ideal for camera events, alerts, summaries, and AI-driven logic
- Integrates cleanly into Home Assistant’s automation editor and UI

This component is required if you want to trigger Tater tools directly from Home Assistant automations.

---

## Local Installation (Advanced)

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

Note:
- Run Tater inside a Python virtual environment so dependencies stay isolated and easy to manage (recommended).
  Quickstart:
  ```bash
  python -m venv .venv
  source .venv/bin/activate
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
  -e TZ=America/Chicago \
  -v /etc/localtime:/etc/localtime:ro \
  -v /etc/timezone:/etc/timezone:ro \
  -e LLM_HOST=127.0.0.1 \
  -e LLM_PORT=11434 \
  -e LLM_MODEL=gemma3-27b-abliterated \
  -e REDIS_HOST=127.0.0.1 \
  -e REDIS_PORT=6379 \
  -v /agent_lab:/app/agent_lab \
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
  -e TZ=America/Chicago \
  -v /etc/localtime:/etc/localtime:ro \
  -v /etc/timezone:/etc/timezone:ro \
  -e LLM_HOST=https://api.openai.com \
  -e LLM_PORT= \
  -e LLM_MODEL=gpt-4o \
  -e LLM_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx \
  -e REDIS_HOST=127.0.0.1 \
  -e REDIS_PORT=6379 \
  -v /agent_lab:/app/agent_lab \
  ghcr.io/tatertotterson/tater:latest
```
Note: When using ChatGPT, leave LLM_PORT blank.  
Tater will automatically connect using HTTPS without appending a port number.

Tip: The runtime data lives in `/app/agent_lab` inside the container.  
If you don’t mount it to the host, `/agent_lab` data can be lost when the container is rebuilt or updated.

Unraid note: add a container path mapping for `/app/agent_lab` to a persistent share (e.g., `/mnt/user/appdata/tater/agent_lab`) so you don’t lose Agent Lab data during container updates.
Unraid note: also set `TZ` and map `/etc/localtime` + `/etc/timezone` if you want local time inside the container.

### 3. Access the Web UI

Once the container is running, open your browser and navigate to:

[http://localhost:8501](http://localhost:8501)

The Streamlit-based web UI will be available for interacting with Tater.

---


https://github.com/user-attachments/assets/9138f485-ccd6-46e0-9295-f5617c079fea
