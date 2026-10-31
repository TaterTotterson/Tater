# Tater v2.0.0

Tater v2.0.0 is a major update to Echo satellite management, screen media,
voice and model configuration, and the native macOS runtime. It is the companion
app release for Tater Echo Firmware v4.0.0.

## What's Changed

### Echo Satellite Management

- Added per-satellite Remote Maintenance controls with an off-by-default SSH
  switch, device-specific login details, and readable password controls.
- Added Do Not Disturb with manual control, overnight-capable daily schedules,
  and the Do Not Disturb Verba. DND blocks music, announcements, wake sounds,
  and non-critical playback while keeping timers, alarms, and voice control
  available.
- Added 12-hour and 24-hour clock selection for Echo screens, including AM/PM
  display in 12-hour mode.
- Satellite cards now identify Wi-Fi or Ethernet connections and clearly mark
  Wi-Fi rescue mode when a wired connection needs attention. Native satellite
  volume is also available directly on each card.
- Added coordinated Echo system OTA support so Tater can validate, stage, and
  track firmware updates that include the Linux root system, not only the app
  partition.

### Echo Screens, Cameras, and Climate

- Native Echo cameras now use display-aware streaming profiles for Sat Chat and
  camera viewing instead of treating live video as repeated still images.
- Added a provider-neutral full-screen video service for compatible Echo screens
  and integrated TVs, with room-aware target selection, playback state, and
  pause, resume, stop, progress, completion, and error handling.
- Added a per-satellite thermostat selector alongside weather and room-temperature
  settings. Automatic mode prefers a thermostat in the satellite's room, while
  an explicit selection keeps screen controls scoped to that thermostat.
- Refined screen weather and climate payloads, room sensor limits, camera
  directories, and reconnect-time state restoration.

### Voice and Speech

- Preserves ordered wake-boundary audio for speech recognition while keeping
  wake audio out of command VAD decisions, improving commands that begin as the
  wake finishes.
- Split Listening from Speech settings so STT, acceleration, VAD, wake
  verification, reply voices, and announcement voices are easier to configure
  and diagnose.
- Reply and announcement voices can reuse each other or keep independent shared
  voice samples. Pocket TTS, Qwen3-TTS, and OmniVoice now use one portable sample
  per profile, with automatic reference transcription where supported.
- Retains incremental reply streaming for supported satellites and improves
  native playback preparation, fallback reporting, and model warmup status.

### Models and Interface

- Standardized built-in local LLM and multimodal inference on llama.cpp GGUF,
  while retaining remote OpenAI-compatible, remote llama.cpp, and Spud Link
  routes. The Models browser now focuses on compatible GGUF downloads.
- Refined llama.cpp context sizing, cache namespaces, multi-slot concurrency,
  and a five-slot layout that gives Base, every Hydra head, and Vision stable
  prompt-cache affinity inside the shared KV pool,
  speculative decoding controls, reasoning controls, process isolation, and
  runtime progress reporting.
- Unified Chat, Settings, runtime status, authentication, and navigation under
  the shared Vue app shell, including browser history and direct settings routes.
- Reworked the Models workspace around the actual workflow: Models, LLM,
  Listening, Speech, Wake Word, Vision, Audio & Video, and identity engines.

### Native macOS App

- Added **Move Tater Data…** for relocating `agent_lab` to another folder or
  external drive with capacity checks, visible progress, copy verification,
  rollback on startup failure, and recoverable cleanup of the old location.
- A missing selected drive now produces an explicit recovery message instead of
  silently creating a new empty data folder.
- Improved launcher startup, shutdown, setup fingerprinting, stale llama.cpp
  cleanup, and replacement of an older app during updates.

### v2 Model Migration

- Built-in local Transformers and MLX LLM/VLM runtimes have been retired in
  favor of llama.cpp GGUF. Users of those providers should download or select a
  GGUF model after updating; remote model providers remain supported.
- Kokoro and Piper selections migrate to Pocket TTS. Chatterbox is retired;
  existing Chatterbox selections fall back to a supported TTS backend and
  should be reviewed once in **Settings → Models → Speech**.
- Setup removes retired MLX and managed Chatterbox runtime files while preserving
  downloaded user data, settings, integrations, and supported model files.

## Tater Echo Firmware v4.0.0

Tater v2.0.0 includes the app-side controls for the separate
[Tater Echo Firmware v4.0.0](https://github.com/TaterTotterson/Tater-Echo-Firmware/releases/tag/v4.0.0)
release.

- Firmware v4 adds automatic USB Ethernet and powered-adapter support, preferring
  Ethernet when healthy and retaining Wi-Fi as an automatic rescue path.
- It also adds SSH maintenance, DND enforcement, screen clock formats, improved
  camera/video handling, Tater boot branding, and coordinated Linux system OTA.
- Any Echo satellite on firmware earlier than v4 needs one USB/TWRP installation
  to establish the new system and recovery baseline. After that, future app and
  Linux system updates can be delivered through Tater OTA; boot-logo changes
  remain USB-only.
