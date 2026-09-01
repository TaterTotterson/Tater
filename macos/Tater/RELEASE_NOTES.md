# Tater v1.1.21

Tater v1.1.21 restores Gemma 4 MTP local models after an upstream llama.cpp
loader regression and makes Tater's native llama.cpp builds reproducible.

## What's Changed

### Reliable llama.cpp Runtime

- Rolls llama.cpp back to the known-good revision immediately before the
  upstream NextN/MTP layer validation regression.
- Restores Gemma 4 assistant sidecars whose layers are all MTP prediction
  layers, preventing the native `llama-server` exit `-6` during model loading.
- Pins macOS packages, private runtime setup, CPU Docker images, and NVIDIA
  Docker images to the same tested llama.cpp revision instead of moving
  `master`.
- Keeps the llama.cpp revision configurable for development while making
  release builds deterministic.
- Adds regression coverage that prevents release build paths from silently
  returning to an unpinned llama.cpp checkout.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.21 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.21` or `latest` for the CPU image and
  `v1.1.21-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
