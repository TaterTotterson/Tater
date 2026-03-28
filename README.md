<div align="center">
  <img src="images/tater_hydra_logo.png" alt="Tater AI Assistant" width="300"/>
</div>

**Tater** is a local AI assistant powered by Hydra and local AI models, giving you a powerful set of AI-driven tools. It includes a WebUI for setup and private chats, and works across **Discord**, **Home Assistant**, **HomeKit**, **IRC**, **macOS**, **Matrix**, **Telegram**, and even the **OG Xbox via XBMC4Xbox**

Main website: [taterassistant.com](https://taterassistant.com)

---

## Hydra
Tater is powered by Hydra, a four-head system:

- Astraeus - sees
- Thanatos - executes
- Minos - judges
- Hermes - speaks

Loop: plan -> execute -> judge -> respond.

Chat turns stay lightweight: Astraeus handles conversational awareness, while Thanatos/Minos/Hermes only engage when execution is needed.

Model routing:

- Base model settings (`Hydra LLM Host`, `Hydra LLM Port`, `Hydra LLM Model`) define AI Calls.
- You can add multiple Base servers; regular AI calls rotate round-robin.
- Beast Mode off: all heads use the Base server pool.
- Beast Mode on: AI Calls still use the Base pool, while Chat/Astraeus/Thanatos/Minos/Hermes each use their own model settings.

---

## 🛒 Tater Shop (Unified Catalog)

Tater uses **Tater Shop** to manage:

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

Unraid note:
- Add container path mappings for `/app/agent_lab` and `/app/.runtime` to persistent shares (for example `/mnt/user/appdata/tater/agent_lab` and `/mnt/user/appdata/tater/runtime`) so you don’t lose Agent Lab data or Redis setup config during container updates.
- Also set `TZ` and map `/etc/localtime` + `/etc/timezone` if you want local time inside the container.

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
- A local LLM runtime (such as **Ollama**, **LocalAI**, **LM Studio**, or **Lemonade**)
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

4. **First-Run Redis Setup**

You no longer configure Redis in `.env`.

Start TaterOS, open the WebUI, and a **Redis Setup** popup will appear automatically if Redis is not configured yet.
Enter:
- Redis host
- Redis port
- Optional auth (`username` / `password`)
- Optional TLS settings

Those connection settings are saved locally by TaterOS for future boots.

After starting TaterOS, configure your local LLM endpoint in **Settings**:
- `Hydra LLM Host`
- `Hydra LLM Port`
- `Hydra LLM Model`
- Optional: add more Base servers for round-robin regular AI calls.
- Optional: enable `Beast Mode` and set per-head model settings for Chat/Astraeus/Thanatos/Minos/Hermes.

Those values are stored in Redis and used by Hydra at runtime.

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

## Docker

### 1. Pull the Image

Pull the prebuilt image with the following command:

```bash
docker pull ghcr.io/tatertotterson/tater:latest
```

### 2. Run Container

Redis settings are configured in the WebUI setup popup (not via `.env`).

Important for Docker persistence:
- Add a path mapping for `/app/agent_lab` (container) -> `/mnt/user/appdata/tater/agent_lab` (host example).
- Without this mapping, data in `/agent_lab` (logs/downloads/documents/workspace) can be lost on container rebuilds/updates.
- Add a path mapping for `/app/.runtime` (container) -> `/mnt/user/appdata/tater/runtime` (host example).
- Without this mapping, Redis setup popup settings can be lost on container rebuilds/updates.

---

Example: local setup
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
  -v /agent_lab:/app/agent_lab \
  -v /tater_runtime:/app/.runtime \
  ghcr.io/tatertotterson/tater:latest
```
---

After the container is running, open TaterOS, complete the Redis setup popup, then configure local LLM endpoint/model in **Settings**:
- `Hydra LLM Host`
- `Hydra LLM Port`
- `Hydra LLM Model`
- Optional: add more Base servers for round-robin regular AI calls.
- Optional: enable `Beast Mode` and set per-head model settings for Chat/Astraeus/Thanatos/Minos/Hermes.

Tip: Redis setup popup config is stored at `/app/.runtime/redis_connection.json` inside the container.

Optional: if you want a custom config file location, set `TATER_REDIS_CONFIG_PATH` and mount that target path from the host.

Access-log note: `run_ui.sh` now starts Uvicorn with `--no-access-log` to suppress per-request lines.

### 3. Access the Web UI

Once the container is running, open your browser and navigate to:

[http://localhost:8501](http://localhost:8501)

---


https://github.com/user-attachments/assets/9138f485-ccd6-46e0-9295-f5617c079fea
