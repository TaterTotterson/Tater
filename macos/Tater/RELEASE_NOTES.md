# Tater v1.1.23

Tater v1.1.23 makes Music Core's AirPlay support ready on clean native
installs and returns llama.cpp builds to the latest upstream version now that
the temporary Gemma 4 MTP loader regression is fixed.

## What's Changed

### Ready-to-Use AirPlay

- Installs and verifies the complete native AirPlay runtime during local Tater
  setup: Shairport Sync 5.2.1, cliairplay 0.4.12, FFmpeg, and discovery support.
- Adds the same pinned and checksum-verified Shairport Sync receiver build for
  native Linux installs that Tater already uses in Docker and on macOS.
- Grants only the AirPlay sender executable access to UDP ports 319 and 320 on
  Linux hosts that restrict AirPlay 2's PTP clock ports.
- Falls back to Tater's managed FFmpeg package when a system FFmpeg executable
  is unavailable, including lightweight Edge installs.
- Rejects incompatible Shairport Sync installations instead of mixing an old
  or incomplete receiver with Music Core.

### Self-Contained macOS AirPlay

- Bundles the native AirPlay sender and receiver inside Tater.app so a clean
  Mac does not need the manual Homebrew setup used during early testing.
- Extends the app's normal startup environment check to verify the receiver,
  sender, FFmpeg, and local-network discovery dependencies before Tater starts.
- Automatically runs setup when a Python-side AirPlay dependency is missing or
  stale, while reusing existing valid runtime components for source installs.

### Latest llama.cpp Builds

- Returns local setup, macOS packages, CPU Docker images, and NVIDIA Docker
  images to upstream llama.cpp `master` after the Gemma 4 MTP fixes landed.
- Keeps `TATER_LLAMA_CPP_REF` available as an emergency rollback or development
  override without holding normal releases to an older revision.
- Verifies that every release build path follows the same upstream default.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.23 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.23` or `latest` for the CPU image and
  `v1.1.23-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
