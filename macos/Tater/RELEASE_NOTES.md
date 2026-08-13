# Tater v1.0.10

Tater v1.0.10 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Music Player Play Button

- The orange play button now keeps its intended compact circular size instead
  of expanding and disrupting the slim Music Core player layout.
- The play triangle has explicit dimensions for consistent rendering in the
  macOS WebKit app and responsive browser layouts.

## Updating

- macOS users already running v1.0.1 through v1.0.9 can install v1.0.10 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.10` or `latest` for the CPU image and
  `v1.0.10-nvidia` or `nvidia` for the NVIDIA image.
