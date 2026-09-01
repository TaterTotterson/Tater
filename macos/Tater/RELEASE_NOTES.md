# Tater v1.1.20

Tater v1.1.20 gives compatible single satellites the same buffered announcement
playback used by stereo pairs, eliminating crackling when an automation speaks
over a background audio loop.

## What's Changed

### Clean Single-Satellite Announcements

- Routes capable single satellites through Tater's buffered media-session and
  audio-overlay path instead of the older on-device audio-scene mixer.
- Keeps background loops synchronized while ducking them beneath automation
  speech, then stops the loop when the announcement finishes.
- Uses the same scheduled playback and completion reporting already proven by
  stereo pairs.
- Retains the existing audio-scene and simple playback routes for older
  satellite firmware that does not support the buffered path.
- Adds regression coverage for modern satellites, legacy firmware, playback
  failures, synchronized overlay timing, ducking, and completion handling.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.20 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.20` or `latest` for the CPU image and
  `v1.1.20-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
