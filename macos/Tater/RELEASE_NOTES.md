# Tater v1.5.2

Tater v1.5.2 adds direct Google Cast playback support and improves guided
setup for downloadable integrations.

## What's New

### Google Cast Playback

- Adds PyChromecast to the standard, edge, CPU Docker, and NVIDIA Docker
  runtimes so Google Cast support is available wherever Tater runs.
- Verifies the Cast dependency during setup, container builds, and startup,
  with a clear recovery message if the runtime is incomplete.
- Declares Google Cast's Bonjour service on macOS so Tater can discover Cast
  TVs, speakers, and groups on the local network.
- Supports the new Google Cast integration from Tater Integrations, including
  automatic discovery, optional manual hosts, direct media playback, volume,
  pause, resume, seek, mute, and stop controls.
- Supports the new Cast Media Verba from Tater Shop. Requests such as
  “create a song and play it on the office TV” can pass the generated audio or
  video artifact directly into Cast playback and resolve the TV by its natural
  device or room name.

### Integration Setup

- Adds guided integration-settings steps so integrations can present clearer
  setup instructions and configuration flows.

## Updating

- macOS users already running v1.0.1 or later can install v1.5.2 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.5.2` or `latest` for the CPU image and
  `v1.5.2-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
