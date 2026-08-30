# Tater v1.1.15

Tater v1.1.15 gives Spudex and voice settings a cleaner Tater-themed interface,
expands Spud Link voice and identity routing, and improves Ryzen AI setup and
runtime reliability.

## What's Changed

### Spudex Workbench

- Refreshes Spudex with a warmer Tater-themed workbench and more compact
  controls throughout.
- Places chat and the read-only command output terminal side by side so the
  conversation stays clean while runtime activity remains visible and
  scrollable.
- Combines tracked processes and session controls into one slim status bar.
- Rebuilds Manual Session as a full terminal-style workspace with an inline
  command prompt, a correctly sized Keep Running option, and no redundant
  recent-command panel.
- Repairs oversized checkboxes and tightens the Settings layout for clearer,
  more consistent controls.

### Voice, Announcements, and Spud Link

- Organizes speech settings into focused Listening & STT, Reply Voice,
  Announcements, and Playback & Test tabs.
- Lets announcements reuse the complete direct-reply TTS setup or use an
  independent backend, model, gain, cloned voice, language, and voice
  instructions.
- Keeps direct-reply and announcement clone audio in separate managed profiles
  so changing or removing one does not affect the other.
- Adds Spud Hub speech-end detection as an independently routable model and
  streams microphone audio to the Hub for live endpointing, with automatic
  local fallback when the connection is interrupted.
- Reuses the Hub stream's final transcription when STT is also routed there,
  avoiding duplicate speech processing.
- Makes WebRTC speech-end detection require sustained speech by default,
  reducing false starts from isolated noisy audio frames.
- Realigns a late stereo-group member to the shared playback timeline when
  supported, while leaving ordinary startup jitter alone.

### Face ID, Speaker ID, and People

- Adds manual Face ID enrollment from an uploaded photo or a camera capture in
  the People panel.
- Makes Spud Hub Face ID and Speaker ID stateless embedding services: images
  and audio can be processed on the Hub while People links, face galleries,
  speaker profiles, and enrollment samples remain on the calling Tater.
- Records embedding-model metadata and only compares compatible Face ID and
  Speaker ID vectors, preventing stale or mismatched models from producing
  incorrect identity matches.
- Requires image-backed face observations and preserves the saved face crop
  used by each local identity profile.
- Clarifies routed identity ownership and model location throughout the model
  and Spud Link interfaces.

### Ryzen AI, Setup, and Core Forms

- Updates Ryzen AI setup for the current ROCm 10 stack and configures AMD GPU
  access during installation.
- Improves Gemma 4 Vulkan stability on `gfx1150` Ryzen AI systems and uses
  memory-mapped model loading where supported.
- Selects a supported Python version during setup, repairs missing virtual
  environment support, and handles CMake 4 when building Python dependencies.
- Adds reusable camera capture support to Core forms opened over HTTPS or
  localhost.
- Adds refreshed Tater ecosystem artwork for Mini AI PC, Spud Hub, and Spudlet
  deployments.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.15 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.15` or `latest` for the CPU image and
  `v1.1.15-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
