# Tater v1.1.5

Tater v1.1.5 expands local speech and media support, improves satellite volume
control, and brings the runtime changes needed by the latest Awareness,
Automation, Music, and integration releases.

## What's Changed

### Speech and Media Models

- Added managed local Qwen3-TTS and OmniVoice backends with voice-clone audio
  uploads, optional transcripts, model warmup, streaming output, and loaded
  model status.
- Improved Pocket TTS startup, prepared-voice reuse, streaming, fallback, and
  cache cleanup.
- Added Audio Understanding and Video Understanding model settings. Each role
  can reuse a capable base model or load its own selected model at startup.
- Added `audio_analyze` and `video_analyze` kernel tools for understanding
  explicit audio files and short video clips.
- Moved image and video model choices into the unified Vision section and
  improved the Models navigation order.

### Awareness, Video, and Face ID

- Core media now supports browser range requests, playable camera clips, and a
  clean return to the event snapshot after playback.
- Core edit forms refresh dependent integration, device, camera, and media
  choices immediately.
- Face ID now uses Tater's regular managed environment instead of a separate
  optional requirements set. Existing installations clean up the retired Face
  ID environment and legacy firmware workspace during setup.

### Satellites and Runtime Reliability

- Added compact volume sliders to satellite cards and synchronized physical
  Sat1 buttons and VoicePE encoder changes back to each device's saved volume.
- Improved integration capability refreshes so newer device state is not
  overwritten by an older background scan.
- Reduced unnecessary Redis persistence work and bounded background rewrites.
- Improved macOS shutdown, installer replacement, and managed-process cleanup.
- Fixed SpeechBrain emotion-model cache placement.
- NVIDIA Docker now exposes PyTorch's NCCL runtime to the bundled llama.cpp
  server, allowing Qwen3-ASR to start without removing multi-GPU LLM support.

## Companion Updates

The following already-published companion versions are designed to use the new
runtime behavior:

- Tater Shop: Awareness Core 4.10.0, Automation Core 1.5.0, and Music Core
  3.4.2. These add image/video event handling, sensor-camera pairing,
  capability-aware triggers, optional event notifications with media and Face
  ID context, richer announcement options, and persistent music volume.
- Tater Integrations: Home Assistant 1.3.3 and UniFi Protect 1.4.0 add camera
  clip support and updated device capabilities.
- Tater Native Firmware 0.3.11 and Tater Satellite Home Assistant integration
  0.3.10 add physical volume synchronization; the firmware also improves OTA
  recovery and wake-chime timing.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.5 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.5` or `latest` for the CPU image and
  `v1.1.5-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
