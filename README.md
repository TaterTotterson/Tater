<div align="center">
  <a href="https://taterassistant.com">
    <img src="images/tater-logo-primary.png" alt="Tater AI Assistant" width="440"/>
  </a>
</div>
<p align="center">
  <a href="https://taterassistant.com">
    <img alt="Visit Tater Assistant" src="https://img.shields.io/badge/Tater%20Assistant-Visit%20Website-F28C28?style=for-the-badge&logo=googlechrome&logoColor=white" />
  </a>
  <a href="https://discord.gg/w52namKyXT">
    <img alt="Join the Tater Assistant Discord" src="https://img.shields.io/badge/Discord-Join%20the%20Community-5865F2?style=for-the-badge&logo=discord&logoColor=white" />
  </a>
</p>

**Tater** is a local-first AI platform that can run local models through **llama.cpp**, **Hugging Face Transformers**, and **MLX**, or connect to local OpenAI-compatible APIs. It supports voice satellites like **VoicePE**, **Sat1**, **S3Box**, and **ReSpeaker XVF3800**, plus portals for **Discord**, **Home Assistant**, **HomeKit**, **IRC**, **macOS**, **Matrix**, **Meshtastic**, **Telegram**, and **XBMC4Xbox**.

---

## Little Spud Companion App

Little Spud connects to your Tater Spud Hub for chat, TTS, STT, and notifications from your iOS or Android device.

<p>
  <a href="https://apps.apple.com/app/little-spud/id6781400718">
    <img alt="Download Little Spud on the App Store" src="https://img.shields.io/badge/App%20Store-Download%20Little%20Spud-0D96F6?style=for-the-badge&logo=apple&logoColor=white" />
  </a>
</p>

<p>
  <a href="https://play.google.com/store/apps/details?id=com.tatertotterson.littlespud.android">
    <img alt="Download Little Spud on Google Play" src="https://img.shields.io/badge/Google%20Play-Download%20Little%20Spud-34A853?style=for-the-badge&logo=googleplay&logoColor=white" />
  </a>
</p>

---

## Tater Bench

Tater Bench compares local models using repeatable Tater-style accuracy and performance tests across engines, speculative decoding modes, and hardware.

<p>
  <a href="https://tatertotterson.github.io/TaterBench/">
    <img alt="View Tater Bench model results" src="https://img.shields.io/badge/Tater%20Bench-View%20Model%20Results-FF7A18?style=for-the-badge&logo=githubpages&logoColor=white" />
  </a>
</p>

---

## 🧩 Tater Architecture

Tater is built around a modular system:

- **Cores** → core systems that extend Tater's capabilities
- **Portals** → integrations with platforms like Discord, Home Assistant, and more
- **Verbas** → AI-driven tools and actions Tater can perform
- **Integrations** → modular provider packages for devices, services, search providers, and external APIs

### Tater Shop

Tater Shop manages the catalogs, versions, metadata, and updates for Tater Cores, Portals, and Verbas.

Core-owned Redis keys must use the Core's canonical namespace: `<core_id>:` or
`<module_key>:`. Standard `<module_key>_settings` and running-state keys are
handled separately. The **Delete data** uninstall option removes those
namespaces automatically. Shared platform namespaces such as `tater:` are not
inferred from a downloaded Core;
historical official exceptions are explicitly audited in `tateros/core_store.py`.

<p>
  <a href="https://github.com/TaterTotterson/Tater_Shop">
    <img alt="Browse Tater Shop" src="https://img.shields.io/badge/Tater%20Shop-Browse%20Extensions-FF7A18?style=for-the-badge&logo=github&logoColor=white" />
  </a>
</p>

### Tater Integrations

Tater Integrations provides modular packages for devices, services, search providers, and external APIs.

<p>
  <a href="https://github.com/TaterTotterson/Tater_Integrations">
    <img alt="Browse Tater Integrations" src="https://img.shields.io/badge/Tater%20Integrations-Browse%20Packages-FF7A18?style=for-the-badge&logo=github&logoColor=white" />
  </a>
</p>

---

# Installation

Choose the installation that matches your system:

- **macOS app**, **Unraid**, and **Home Assistant** provide guided installation options.
- **Docker** is the easiest choice for most other Linux servers.
- **Local installation** is best when you want direct access to the hardware and Python environment.

> **Model note:** Tater is designed around local models and local OpenAI-compatible servers such as Ollama, LM Studio, LocalAI, and Lemonade.

## macOS App Installation

<p>
  <a href="https://taterassistant.com">
    <img alt="Download Tater for macOS" src="https://img.shields.io/badge/macOS-Download%20Tater-000000?style=for-the-badge&logo=apple&logoColor=white" />
  </a>
</p>

1. **Download the latest macOS installer**

   [Download the latest Tater for macOS](https://taterassistant.com)

2. **Install Tater**

   Open the DMG, then drag **Tater.app** into **Applications**.

3. **Launch Tater**

   Open **Tater** from Applications. On first launch, the app prepares its private runtime under:

   ```text
   ~/.taterassistant/
   ```

   The app stores its managed Python runtime, virtual environment, runtime settings, logs, updates, and `agent_lab` data there. It does not use this source checkout's `.venv`, `.runtime`, or `agent_lab` folders. The app also includes the pinned AirPlay sender and receiver, and its startup environment check repairs any missing Python-side AirPlay support automatically.

4. **Finish setup in TaterOS**

   The app listens on `0.0.0.0:8501` and opens `127.0.0.1:8501` in the native window. If Python 3.11 is not already available, the launcher downloads a standalone CPython 3.11 runtime into `~/.taterassistant/python/` and uses it to build the private venv.

Closing the window keeps Tater running in the menu bar. Use the menu bar item to reopen Tater, open it in a browser, stop, restart, show logs, check for updates, install available updates, or quit.

Once the WebUI is up, continue to **Post-Install Setup** below.

## Unraid Installation

<img width="100" height="44" alt="unraid_logo_black-339076895" src="https://github.com/user-attachments/assets/87351bed-3321-4a43-924f-fecf2e4e700f" />

Tater is available in the **Unraid Community Apps** store.

You can install **Tater** directly from the Unraid App Store with a one-click template.

Unraid note:

- Add container path mappings for `/app/agent_lab` and `/app/.runtime` to persistent, preferably cache-backed storage.
- Also set `TZ` and map `/etc/localtime` plus `/etc/timezone` if you want local time inside the container.

Once the Unraid containers are installed and running, continue to **Post-Install Setup** below.

## Home Assistant Installation

A dedicated Home Assistant add-on repository is available here:

https://github.com/TaterTotterson/hassio-addons-tater

Click the button below to add the repository to Home Assistant:

[![Add Repository to Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](
https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https://github.com/TaterTotterson/hassio-addons-tater
)

Once added, the **Tater AI Assistant** add-on will appear in the Home Assistant Add-on Store.

Install order:

1. Install Tater AI Assistant.
2. Configure your LLM settings in the Tater add-on.
3. Start Tater.

Once the add-ons are running, continue to **Post-Install Setup** below.

## Reachy Mini Installation

<p>
  <a href="https://huggingface.co/spaces/TaterTotterson/reachy_tater_sat">
    <img alt="Install Reachy Tater Satellite" src="https://img.shields.io/badge/Reachy%20Mini-Install%20Tater%20Satellite-FFD21E?style=for-the-badge&logo=huggingface&logoColor=black" />
  </a>
</p>

The **Reachy Tater Satellite** app turns Reachy Mini Wireless into a native voice satellite for an existing Tater server. Microphone, speaker, local wake-word detection, user tracking, expressive motion, and optional vision snapshots remain on Reachy, while the robot connects to Tater through its authenticated native satellite connection.

Install path:

1. Install [Reachy Tater Satellite](https://huggingface.co/spaces/TaterTotterson/reachy_tater_sat) from the Reachy Mini app store and start it.
2. Open the app settings from Reachy's web interface and enter the address of your running Tater server.
3. Create a satellite pairing code in Tater, enter it once in the Reachy settings, and save the connection.
4. Optionally enable **Allow vision snapshots** for the Reachy Vision Verba, then say **Hey Reachy**.

## Docker Installation

Use this method on a Linux server with Docker installed.

### 1. Create persistent storage

Run these commands from the directory where you want to keep Tater's data:

```bash
mkdir -p tater-data/agent_lab tater-data/runtime
```

- `agent_lab` stores models, documents, logs, and Tater's internal Redis data.
- `runtime` stores settings and native satellite pairing credentials.

Keep both directories when you update or recreate the container. Fast local storage is recommended; avoid network-mounted or unusually slow storage for `agent_lab`.

### 2. Start Tater

```bash
docker pull ghcr.io/tatertotterson/tater:latest

docker run -d --name tater_webui \
  --restart unless-stopped \
  --network host \
  --cap-add NET_BIND_SERVICE \
  -e TZ=America/Chicago \
  -e HTMLUI_PORT=8501 \
  -v "$(pwd)/tater-data/agent_lab:/app/agent_lab" \
  -v "$(pwd)/tater-data/runtime:/app/.runtime" \
  ghcr.io/tatertotterson/tater:latest
```

Change `TZ` if needed. Tater uses host networking for WebUI, voice, discovery, and media services.

### 3. Open TaterOS

Open one of these addresses:

- [http://localhost:8501](http://localhost:8501) on the server
- `http://<server-ip>:8501` from another device on the same network

If port 8501 is already in use, change `HTMLUI_PORT` in the command. Continue to **Post-Install Setup** after TaterOS opens.

### NVIDIA Docker

The NVIDIA image is available for amd64 systems with an NVIDIA driver and NVIDIA Container Toolkit installed:

```bash
docker pull ghcr.io/tatertotterson/tater:nvidia
```

Use the Docker command above, add `--gpus all`, and replace the final image name with `ghcr.io/tatertotterson/tater:nvidia`.

## Local Installation

Use this method on Linux or macOS when you want Tater to run directly on the host.

### 1. Download Tater

```bash
git clone https://github.com/TaterTotterson/Tater.git
cd Tater
```

### 2. Run setup

```bash
sh setup_tater.sh
```

The setup menu asks which runtime fits your system, creates `.venv`, installs dependencies, and saves the selected profile. Tater supports Python 3.11 through 3.13; on supported Linux systems, setup can install a private Python runtime when the system version is unsuitable.

| Profile | Choose it for |
| --- | --- |
| CPU | Most Linux PCs and generic ARM systems |
| macOS | Apple Silicon Macs |
| NVIDIA | Linux PCs and servers with NVIDIA GPUs |
| AMD ROCm | Supported Linux systems with AMD GPUs or Ryzen AI |
| Jetson | NVIDIA Jetson systems |
| Thor | Jetson Thor systems |
| Edge | Pi-class or remote-only systems that connect to a Spud Hub |

To skip the menu, pass the profile name directly—for example:

```bash
sh setup_tater.sh cpu
```

The Edge profile requires the operating system's `redis-server` package. If macOS setup reports missing build tools, install `ffmpeg` and `cmake` with Homebrew and rerun setup.

### 3. Start Tater

```bash
sh run_ui.sh
```

Tater listens on `0.0.0.0:8501` by default. Open [http://localhost:8501](http://localhost:8501), or use `http://<computer-ip>:8501` from another device. To use a different port:

```bash
HTMLUI_PORT=8601 sh run_ui.sh
```

Continue to **Post-Install Setup** after TaterOS opens. Model downloads and voice acceleration are configured inside TaterOS under **Settings -> Models** and **Settings -> Voice Pipeline**.

---

## Post-Install Setup

After Tater is running, open TaterOS and finish the first-run setup:

1. Configure your base model in **Settings -> Models -> LLM / Vision**:
   - choose `OpenAI-Compatible API` for a local server such as Ollama, LM Studio, LocalAI, Lemonade, or vLLM
   - choose `Hugging Face Transformers` to load a local model directly inside Tater
   - choose `llama.cpp GGUF` to load a GGUF model through Tater's native llama.cpp engine
   - choose `MLX LM (Apple Silicon)` to load an MLX model directly on an Apple Silicon Mac
   - for built-in local providers, download models from the Hugging Face mini-tab first, then select the downloaded model from the Settings mini-tab
   - for OpenAI-compatible providers, set the endpoint host/port and model name
2. Optional:
   - add more Base servers for round-robin regular AI calls
   - enable `Beast Mode` and set per-head model settings for Astraeus/Hermes

Hydra model settings are saved by TaterOS and used at runtime. Base, Spudex, Beast Mode routing, and Vision can each use the selected built-in local providers or OpenAI-compatible providers.

### Local Models

- Download local Hugging Face Transformers, llama.cpp GGUF, or MLX models from the Hugging Face mini-tab first, then select them from Settings.
- Model caches live under `agent_lab/models/llm/` by default:
  - `huggingface` for Transformers
  - `llama-cpp` for GGUF models and matching `mmproj*.gguf` vision projectors
  - `mlx` for MLX text and vision models
- The Hugging Face browser uses the token saved in **Integration Manager -> Hugging Face** for private/gated models and better Hub rate limits.
- llama.cpp uses the native `llama-server` engine built by setup. It uses GPU offload by default when the installed build supports it. Set `TATER_LLAMA_CPP_N_GPU_LAYERS=0` for CPU-only or `TATER_LLAMA_CPP_SERVER_BIN` to point at a custom llama-server binary.
- MLX is intended for Apple Silicon Macs. Use llama.cpp GGUF on Linux, Raspberry Pi, NVIDIA, AMD/ROCm, Jetson, or other non-Apple-Silicon devices.

### Vision

- Vision can use an OpenAI-compatible API, the loaded Base model, or a dedicated local vision model.
- If Base is already loaded and vision-capable, Tater reuses it instead of loading the same model twice.
- Dedicated vision models are managed separately from Base.

### Advanced Notes

- Local context length is configured in **Settings -> Models -> LLM / Vision**.
- Thinking suppression is enabled by default for local providers when supported.
- `run_ui.sh` starts Uvicorn with `--no-access-log` to suppress per-request log spam. Shutdown waits at most eight seconds for long-lived WebSocket and event-stream connections before cancelling them, so a stale satellite or browser connection cannot block a restart. Set `HTMLUI_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS` to override that connection-drain deadline.
