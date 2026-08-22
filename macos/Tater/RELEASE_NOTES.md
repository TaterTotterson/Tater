# Tater v1.1.10

Tater v1.1.10 fixes the complete voice-to-music handoff for stereo satellite
pairs, including commands that start new music.

## What's Changed

### Voice and Music

- Skips spoken tool-progress audio when an idle stereo pair is about to begin
  playback, while keeping the normal visual progress state.
- Continues using spoken progress with normal ducking whenever stereo music is
  already active.
- Recovers synchronized session state directly from both live satellites when
  coordinator state is missing.
- Keeps the final spoken confirmation as a synchronized overlay so the new
  music ducks and resumes instead of being replaced.
- Refreshes the recovered pair's clock calibration before starting its TTS
  overlay.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.10 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.10` or `latest` for the CPU image and
  `v1.1.10-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
