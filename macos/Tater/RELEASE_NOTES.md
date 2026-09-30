# Tater v1.2.4

Tater v1.2.4 adds first-class Tater Open WebUI support, clearer Terminal naming,
faster Hydra tool feedback, and more reliable native satellite audio.

## What's Changed

### Tater Open WebUI and Spud Link

- Adds a dedicated Spud Link client role and polished pairing flow for Tater
  Open WebUI.
- Shares `tater/base`, `tater/hydra`, speech-to-text, and text-to-speech while
  keeping the linked WebUI's local terminal on its own host.
- Registers each linked WebUI account as a discoverable identity so it can be
  connected to a Person and receive the correct permissions.
- Adds an optional dedicated coding-model route for Tater Open WebUI Base calls;
  Hydra keeps its normal model and capability routing.

### Terminal and Hydra

- Renames the user-facing Spudex workspace and settings to **Terminal** while
  preserving compatibility with existing configuration and integrations.
- Runs Hydra's live progress generation alongside tool execution so quick tools
  no longer wait for a status sentence before starting.
- Makes progress updates more specific about the requested action and target
  without exposing credentials or internal tool details.

### Native Satellite Audio

- Improves stereo recovery when a satellite rebuffers or rejoins playback,
  including bounded realignment and steadier rate corrections.
- Makes media handoffs and live volume changes session-aware so stale cleanup
  cannot interrupt replacement playback.
- Normalizes background audio for reliable overlays and uses consistent public
  playback URLs for native speech and media assets.

## Updating

- macOS users already running v1.0.1 or later can install v1.2.4 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.2.4` or `latest` for the CPU image and
  `v1.2.4-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
