<div align="center">
  <img src="https://github.com/user-attachments/assets/47c60b47-e10c-44d6-bdc5-1f9da2479980" alt="Tater AI Assistant" width="300"/>
</div>

**Tater** is an AI assistant that connects to any OpenAI-compatible LLM, giving you a powerful set of AI-driven tools. It includes a WebUI for setup and private chats, and works across **Discord**, **Telegram**, **IRC**, **Matrix**, **Home Assistant**, **HomeKit**, and even the **OG Xbox via XBMC4Xbox**

---

## Cerberus Orchestration

Tater now uses **Cerberus**, a closed-loop **Planner -> Doer -> Checker** architecture for tool execution quality:

- **Planner** chooses one next action (normal response or exactly one tool JSON call).
- **Doer** validates and executes tools deterministically with runtime platform gating.
- **Checker** verifies the outcome, then either finalizes, retries one tool call, or asks one short clarification question.

This loop is now the default orchestration path across Tater platform handlers so tool behavior stays consistent everywhere.

### Cerberus Runtime Config (Env Vars)

Cerberus is safe-by-default and bounded unless you explicitly set unlimited values.

- `CERBERUS_DEFAULT_MAX_ROUNDS` (default: `1`)  
  Maximum planner rounds per turn when no per-request override is provided. `0` means unlimited.
- `CERBERUS_DEFAULT_MAX_TOOL_CALLS` (default: `1`)  
  Maximum tool executions per turn when no per-request override is provided. `0` means unlimited.
- `AGENT_STATE_TTL_SECONDS` (default: `604800` / 7 days)  
  TTL for persistent Cerberus state keys (`tater:cerberus:state:<platform>:<scope>`). `0` disables TTL.
- `CERBERUS_MAX_LEDGER_ITEMS` (default: `300`)  
  Max retained entries in each Redis ledger list.
- `CERBERUS_PLANNER_MAX_TOKENS` (default: `500`)  
- `CERBERUS_CHECKER_MAX_TOKENS` (default: `420`)  
- `CERBERUS_DOER_MAX_TOKENS` (default: `380`)  
  Optional token caps for planner/checker/doer calls.

### Cerberus Scope + Ledger

- Scope is resolved per conversation container (`channel`, `room`, `chat`, `device`, `session`) so state does not bleed across rooms/devices.
- Ledger entries are written to:
  - `tater:cerberus:ledger`
  - `tater:cerberus:ledger:<platform>`
- Each entry includes validation details, planner classification, compact tool result, outcome/reason, and timing fields for debugging.

---

## 🌐 Tater Platform Overview

| Platform          | Description |
|-------------------|-------------|
| `discord`         | Full-featured Discord bot that runs compatible plugins and supports rich interactions, media output, and background jobs. |
| `homeassistant`   | Native integration for [Home Assistant](https://github.com/TaterTotterson/Tater-HomeAssistant), allowing Tater to act as a voice-enabled AI assistant through the Assist pipeline and control smart devices directly. |
| `ha_automations`  | Lightweight Home Assistant automation-only endpoint for direct tool execution. Designed for fast, reliable automations like camera events, doorbell alerts, weather summaries, and dashboard sensors. Intended to be used with the [Tater Automations](https://github.com/TaterTotterson/tater_automations) custom component, which provides a native “Call Tater automation tool” action in Home Assistant. |
| `ai_task`         | Built-in scheduled task runner for timed and recurring AI jobs, with delivery routed through notifier platforms (Discord, Telegram, Matrix, IRC, Home Assistant, and more). |
| `homekit`         | Siri / Apple Shortcuts integration for [HomeKit](https://github.com/TaterTotterson/Tater/wiki/How-to-Build-the-%E2%80%9CTater%E2%80%9D-Shortcut), enabling “Hey Siri, ask Tater…” voice commands, per-device conversation sessions, and plugin-backed actions. |
| `irc`             | Lightweight IRC bot that responds to mentions and runs compatible plugins. |
| `matrix`          | Modern Matrix client with end-to-end encryption support, Markdown rendering, and full plugin compatibility — bringing Tater to federated chat networks like Element and Cinny. |
| `telegram`        | Telegram bot integration with chat allowlists, DM user restrictions, queued notifications, media delivery, and plugin-backed tool execution. |
| `xbmc`            | OG Xbox integration for **XBMC4Xbox**, provided by the custom Cortana-powered skin and script at [skin.cortana.tater-xbmc](https://github.com/TaterTotterson/skin.cortana.tater-xbmc), enabling an on-console AI assistant with chat history, quick asks, and plugin-powered actions. |
| `webui`           | Streamlit-based WebUI for chatting, configuring plugins, and managing settings. |


## 🧩 Tater Plugin Ecosystem

Tater now uses a **remote plugin store**.  
Plugins are no longer bundled with Tater — they are installed, updated, and restored automatically from the Tater Shop.

### 🛒 Tater Plugin Store

All plugins, versions, descriptions, and update history now live here:

👉 **https://github.com/TaterTotterson/Tater_Shop**

---

### 🔍 Browsing Plugins

From the WebUI you can:

- Search by name or description  
- Filter by platform (Discord, WebUI, Home Assistant, etc.)  
- See installed vs store versions  
- One-click install, update, remove  
- Bulk “Update All”  

---

### ♻️ Auto-Restore

When Tater starts:

> Any plugin that was **enabled** in Redis but missing on disk  
> is automatically re-downloaded from the store.

No config loss. No manual installs. No volume mapping required.

**Note**:
- Do not use a thinking model with tater
- Tater currently recommends using qwen3-next-80b, qwen3-coder-30b or Gemma3-27b

---

# Installation

<img width="100" height="44" alt="unraid_logo_black-339076895" src="https://github.com/user-attachments/assets/87351bed-3321-4a43-924f-fecf2e4e700f" />

Tater is available in the **Unraid Community Apps** store.

You can install both:
- **Tater**
- **Redis Stack**

directly from the Unraid App Store with a one-click template.

## 🏠 Home Assistant

A dedicated Home Assistant add-on repository is available here:

https://github.com/TaterTotterson/hassio-addons-tater

### Add the Tater add-on repository

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

## 🔌 Home Assistant Integration (Optional Enhancements)

Tater AI supports deeper integration with Home Assistant through a couple of companion repositories. These extend Tater’s usefulness in smart home setups by enabling voice/text conversation control and native automation triggers.

### 📍 Tater-HomeAssistant
https://github.com/TaterTotterson/Tater-HomeAssistant

A Home Assistant **custom integration** that allows Tater to function as a **Conversation Agent** inside Home Assistant’s Assist pipeline. This enables voice or text interactions from Home Assistant to be routed directly to your Tater backend, where plugins can be executed and contextual responses returned.

Key benefits:
- Use Tater as a native **voice and text assistant** within Home Assistant
- Routes Assist queries directly to your running Tater AI backend
- Supports plugins that implement `handle_homeassistant(...)`
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

### Persist Agent Lab data in `/agent_labs` (recommended)

If you want Agent Lab data to survive repo rebuilds/reinstalls, use a dedicated path and symlink:

```bash
sudo mkdir -p /agent_labs/{plugins,platforms,artifacts,documents,downloads,workspace,logs}
cp -a agent_lab/. /agent_labs/ 2>/dev/null || true
rm -rf agent_lab
ln -s /agent_labs agent_lab
```

Note for Agent Lab:
- If you plan to use Agent Lab, run Tater inside a Python virtual environment so Agent Lab dependencies stay isolated and easy to manage (recommended).
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
  -v /agent_labs:/app/agent_lab \
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
  -v /agent_labs:/app/agent_lab \
  ghcr.io/tatertotterson/tater:latest
```
Note: When using ChatGPT, leave LLM_PORT blank.  
Tater will automatically connect using HTTPS without appending a port number.

Tip: The Agent Lab data lives in `/app/agent_lab` inside the container.  
If you don’t mount it to the host, Agent Lab plugins/platforms/artifacts will be lost when the container is rebuilt or updated.

Unraid note: add a container path mapping for `/app/agent_lab` to a persistent share (e.g., `/mnt/user/appdata/tater/agent_labs`) so you don’t lose Agent Lab data during container updates.
Unraid note: also set `TZ` and map `/etc/localtime` + `/etc/timezone` if you want local time inside the container.

### 3. Access the Web UI

Once the container is running, open your browser and navigate to:

[http://localhost:8501](http://localhost:8501)

The Streamlit-based web UI will be available for interacting with Tater.

---


https://github.com/user-attachments/assets/9138f485-ccd6-46e0-9295-f5617c079fea
