<div align="center">
  <img src="images/tater-new-logo.png" alt="Tater AI Assistant" width="300"/>
</div>

**Tater** is an AI assistant that connects to any OpenAI-compatible LLM, giving you a powerful set of AI-driven tools. It includes a WebUI for setup and private chats, and works across **Discord**, **Home Assistant**, **HomeKit**, **IRC**, **macOS**, **Matrix**, **Telegram**, and even the **OG Xbox via XBMC4Xbox**

Main website: [taterassistant.com](https://taterassistant.com)

---

## Cerberus
Tater is powered by the Cerberus Core, a three-headed execution system:

- Astraeus, the Seer, determines the path ahead
- Thanatos, the executor, carries out the work
- Minos, the Arbiter, judges the outcome

Cerberus operates in a loop: foresight -> execution -> judgment -> repeat -> respond.

Chat path:

- Astraeus speaks with awareness
- Thanatos stands down
- Minos is not invoked unless execution occurs

---

## 🛒 Tater Shop (Unified Catalog)

Tater uses **Tater Shop** as the source of truth for:

- **Verbas**
- **Portals**
- **Cores**

This repository no longer keeps static lists of those modules in the README.
Instead, catalogs, versions, metadata, and update paths are managed in:

👉 **https://github.com/TaterTotterson/Tater_Shop**

From the WebUI, use:

- **Verba Manager**
- **Portal Manager**
- **Core Manager**

Each manager supports browse/install/update/remove and startup restore for enabled items that are missing on disk.

---

## Supporting Apps

Some Portals are paired with companion repos/apps that complete the end-user integration:

| Companion Repo/App | Used With | Purpose |
|---|---|---|
| https://github.com/TaterTotterson/hassio-addons-tater | Home Assistant | Home Assistant add-on repository for running Tater + Redis Stack directly inside HAOS/Supervised setups. |
| https://github.com/TaterTotterson/Tater-HomeAssistant | Home Assistant Portal | Conversation Agent integration that routes Home Assistant Assist requests to Tater. |
| https://github.com/TaterTotterson/tater_automations | HA Automations Portal | Native Home Assistant automation actions that call Tater tools directly. |
| https://github.com/TaterTotterson/Tater-MacOS | macOS Portal | Menu bar companion app and bridge client for desktop chat, quick actions, and uploads. |
| https://github.com/TaterTotterson/skin.cortana.tater-xbmc | XBMC Portal | OG Xbox/XBMC4Xbox skin and script integration for on-console Tater access. |
| https://taterassistant.com/portals/homekit.html | HomeKit Portal | Shortcut guide for Siri -> HomeKit bridge -> Tater workflows. |

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

- **Redis Stack** – required for Tater memory, Verbas, and automations
- **Tater AI Assistant** – the main Tater service

#### Install order

1. Install and **start Redis Stack**
2. Install **Tater AI Assistant**
3. Configure your LLM and Redis settings in the Tater add-on
4. Start Tater

This is the recommended setup for most users and provides the smoothest experience.

---

## Local Installation

### Prerequisites
- Python 3.11
- **[Redis-Stack](https://hub.docker.com/r/redis/redis-stack)**
- OpenAI API–compatible LLM app (such as **Ollama**, **LocalAI**, **LM Studio**, **Lemonade**, or **OpenAI API**)
- Docker (optional, for containerized deployment)

### Install Redis Stack (Required)

#### Option 1: Ubuntu/Debian with APT

Install Redis Stack from the official Redis APT repository:

```bash
sudo apt-get install -y lsb-release curl gpg
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
sudo chmod 644 /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
sudo apt-get update
sudo apt-get install -y redis-stack-server
sudo systemctl enable redis-stack-server
sudo systemctl start redis-stack-server
```

Verify Redis is up:

```bash
redis-cli ping
```

Expected output:

```text
PONG
```

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

Run the TaterOS backend/frontend (FastAPI + static HTML/CSS/JS):

```bash
uvicorn tateros_app:app --host 0.0.0.0 --port 8501 --reload --no-access-log
```

Docker-style launcher (also disables access logs by default):

```bash
sh run_ui.sh
```

Then open:

```text
http://127.0.0.1:8501
```

HTML UI now includes:
- Live chat job streaming with tool-status updates
- Verba/Core/Portal shop install, update, remove, and repo management
- Core top tabs built from each core's `CORE_WEBUI_TAB` metadata (`Manage` + dynamic core tabs)
- Startup restore of missing enabled verbas/cores/portals, then autostart of enabled cores/portals

Core HTMLUI tab payload contract:
- Optional per-core function: `get_htmlui_tab_data(redis_client=..., core_key=..., core_tab=...) -> dict`
- Payload keys used by HTMLUI: `summary`, `stats`, `items`, `empty_message`

Startup behavior env toggles (optional):
- `HTMLUI_RESTORE_ENABLED_SURFACES_ON_STARTUP=true|false` (default `true`)
- `HTMLUI_AUTOSTART_ENABLED_SURFACES_ON_STARTUP=true|false` (default `true`)

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

Access-log note: `run_ui.sh` now starts Uvicorn with `--no-access-log` to suppress per-request lines.

Unraid note: add a container path mapping for `/app/agent_lab` to a persistent share (e.g., `/mnt/user/appdata/tater/agent_lab`) so you don’t lose Agent Lab data during container updates.
Unraid note: also set `TZ` and map `/etc/localtime` + `/etc/timezone` if you want local time inside the container.

### 3. Access the Web UI

Once the container is running, open your browser and navigate to:

[http://localhost:8501](http://localhost:8501)

---


https://github.com/user-attachments/assets/9138f485-ccd6-46e0-9295-f5617c079fea
