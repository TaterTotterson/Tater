# Tater v1.2.0

Tater v1.2.0 brings a faster, cleaner, fully Vue-powered interface with major
improvements to models, satellites, extensions, and live system status.

## What's Changed

### A More Live Tater

- Settings and app navigation now update live without full-page refreshes.
- Cleaner cards, forms, popups, process views, and consistent controls across
  the WebUI.
- New color themes, including the original Tater look and a light theme.

### Easier Model Management

- Redesigned LLM, speech, wake word, vision, audio/video, and identity setup.
- A clearer Hugging Face library with multi-model queues, progress, and
  installed-model detection.
- Better speculative-model filtering and visible model reload progress.

### Better Satellites

- Redesigned satellite pairing, settings, firmware updates, stereo pairs, and
  display controls.
- Built-in AirPlay input no longer depends on Music Core.
- New live Bluetooth presence room map with stable device lists and movement
  details.

### Cores, Verba, and Portals

- New trusted-repository browser alongside custom repositories.
- Cleaner install, update, management, and settings experiences.
- Faster portal loading and richer core-tab forms, popups, and action notices.

### Reliability and Performance

- Smoother Chat responses and more efficient Hydra plan-state updates.
- Improved face matching, Beast Mode prompt-cache routing, and live hardware
  telemetry.
- Numerous satellite, audio, model, and UI reliability fixes.

## Updating

- macOS users already running v1.0.1 or later can install v1.2.0 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.2.0` or `latest` for the CPU image and
  `v1.2.0-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
