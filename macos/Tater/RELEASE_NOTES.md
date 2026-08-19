# Tater v1.1.3

Tater v1.1.3 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Qwen3-ASR Local Speech Recognition

- Added experimental **Qwen3-ASR (llama.cpp)** as a local speech-to-text engine
  for Tater voice sessions, partial transcripts, and wake-word verification.
- Tater downloads the Qwen3-ASR 0.6B Q8 GGUF model and audio projector on first
  selection, stores them in the managed STT model cache, and runs them in an
  authenticated loopback-only instance of Tater's bundled `llama-server`.
- The selected Qwen model warms after speech settings are saved and during app
  startup, while switching STT engines stops the dedicated server and releases
  the obsolete model runtime.
- Runtime Stats now reports the active Qwen STT process under Loaded Model
  Entries with its model, projector, acceleration, load time, and estimated
  memory footprint.

### Speech Settings and Wake Verification

- Faster Whisper's engine-specific settings card is now shown only while
  Faster Whisper is the selected STT engine.
- Wake-word verification follows the selected STT engine, including Qwen3-ASR,
  and its statistics now identify the engine used for the latest check.
- Results recorded before an STT-engine change are marked as previous, while
  runtime substitutions are identified as fallbacks instead of appearing to
  use the newly selected engine.

## Updating

- macOS users already running v1.0.1 through v1.1.2 can install v1.1.3 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.3` or `latest` for the CPU image and
  `v1.1.3-nvidia` or `nvidia` for the NVIDIA image.
