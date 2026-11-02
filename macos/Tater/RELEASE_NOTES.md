# Tater v2.0.3

Tater v2.0.3 adds two streaming local speech-recognition engines, a larger
Qwen3-ASR option, and live voice transcripts and replies on compatible Echo
screens when paired with Echo Firmware v4.0.2.

## What's Changed

### Live conversations on Echo screens

- Streams partial user speech and assistant reply text to Checkers, Crown,
  Athena, and Rook through one shared, engine-independent display protocol.
- Keeps the completed request visible while Tater thinks and works, then
  transitions into the reply with ordered, throttled full-state updates.
- Supports non-streaming speech engines too by sending the complete transcript
  or reply as soon as it is available.
- Requires Echo Firmware v4.0.2 for the new on-screen conversation experience;
  older firmware continues to support normal voice operation without it.

### More local speech recognition

- Adds Voxtral Realtime 4B as an experimental streaming llama.cpp speech engine.
- Adds LFM3-Audio 2B as a compact experimental streaming llama.cpp speech engine.
- Automatically downloads the selected models and manages their dedicated
  llama.cpp processes on macOS, Docker, and NVIDIA Docker installations.
- Keeps a persistent realtime wake-check session warm so optional STT wake-word
  verification does not pay a fresh connection cost for every wake.

### Qwen3-ASR model choices

- Adds a choice between the faster Qwen3-ASR 0.6B model and the larger 1.7B
  model for users who prefer additional transcription accuracy.
- Downloads and loads the matching model automatically, and cleanly restarts
  the managed speech runtime when the selection changes.

### Cleaner listening setup

- Removes the older Vosk backend and its dependency now that Tater has stronger
  local speech-recognition choices.
- Extends speech runtime status, warmup, memory reporting, shutdown ownership,
  and diagnostics to the new managed llama.cpp engines.
- Adds focused regression coverage for realtime transcription, wake checks,
  model selection, live display snapshots, and process cleanup.

### Echo Firmware v4.0.2 companion update

- Adds the matching listening, thinking, working, and replying screen flow with
  theme-aware edge lighting and smooth layouts for rectangular and round Echo
  displays.
- Improves the Sendspin music screen with calmer synchronized spectrum motion
  and a stable progress bar on page-flipped displays.
- See the [Echo Firmware v4.0.2 release](https://github.com/TaterTotterson/Tater-Echo-Firmware/releases/tag/v4.0.2)
  for firmware downloads and complete device-side notes.

**Full Changelog**: [v2.0.2...v2.0.3](https://github.com/TaterTotterson/Tater/compare/v2.0.2...v2.0.3)
