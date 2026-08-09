# Tater v1.0.2

Tater v1.0.2 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Tater Wake Word Catalog

- Added **Tater Wake Word Catalog** as a wake-word source under
  **Settings → Models → Wake Word**.
- The catalog picker loads every official model from the Tater Wake Words
  manifest and shows its source version beside the name, such as
  `Hey Tater [V5]`. At release time the catalog contains 665 models across
  V1 through V6.
- Catalog selections use the existing Tater-native custom wake-model protocol,
  so connected satellites receive the chosen model immediately without a
  firmware protocol change.
- Existing catalog models are recognized when settings are reopened. Catalog
  downloads are cached, stale cached results remain available during a GitHub
  outage, and selections are restricted to official Tater Wake Words URLs.

### Reachy TTS Reliability

- Fixed Reachy's standalone and ambient speech using the wrong TTS backend when
  no active voice-session runtime was present.
- Reachy ambient comments and other standalone synthesis now honor Tater's
  configured TTS backend and voice instead of incorrectly falling back to
  Wyoming.

## Updating

- macOS users already running v1.0.1 can install v1.0.2 through Tater's normal
  updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.2` or `latest` for the CPU image and
  `v1.0.2-nvidia` or `nvidia` for the NVIDIA image.
