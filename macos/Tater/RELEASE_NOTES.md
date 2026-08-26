# Tater v1.1.12

Tater v1.1.12 improves local model cleanup, native satellite firmware updates,
and the People settings layout.

## What's Changed

### Local Models

- Cleans up Tater-managed llama.cpp servers when the backend or macOS app
  starts and stops so crashed workers cannot leave duplicate model servers
  consuming memory.
- Limits cleanup to Tater's own bundled llama.cpp processes and model aliases,
  leaving unrelated llama.cpp servers alone.

### Native Satellite Updates

- Keeps native OTA progress active through the satellite reboot and confirms
  the device reconnects on the requested firmware before showing 100% and
  Complete.
- Preserves the satellite's OTA status log across reconnects and reports clear
  failures when the update times out or the returned firmware version does not
  match.

### People UI

- Keeps People, Faces, and Identities together on one tab row, with compact
  horizontal scrolling on narrower screens.
- Replaces the cramped saved-face checkbox list with a themed review gallery
  where face images can be clicked directly, selected together, moved to the
  correct person or profile, or permanently deleted.
- Adds clear selected-image highlighting, selection counts, Select All and
  Clear controls, and responsive gallery actions for smaller screens.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.12 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.12` or `latest` for the CPU image and
  `v1.1.12-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
