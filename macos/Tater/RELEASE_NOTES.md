# Tater v1.1.4

Tater v1.1.4 is a patch release for Docker and macOS.

## What's Changed

### Awareness Source Editing

- Changing the integration while editing an Awareness source now refreshes the
  optional-camera choices immediately.
- Camera-dependent choices, including image and video description modes, stay
  synchronized with the selected camera.
- The shared fix also supports dependent selections in other Core edit popups.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.4 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.4` or `latest` for the CPU image and
  `v1.1.4-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
