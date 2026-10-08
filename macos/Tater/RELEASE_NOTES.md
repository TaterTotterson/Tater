# Tater v1.3.1

Tater v1.3.1 makes Music Core playback more informative and resilient, adds
rich Sendspin presentation for capable Echo satellites, and exposes Sat1 audio
output selection.

## What's Changed

### Music and Sendspin

- Sends track details, playback progress, bounded album artwork, track colors,
  loudness, peaks, beat events, and spectrum data to Sendspin players that
  advertise those capabilities.
- Powers Checkers and Rook now-playing displays and Biscuit music LEDs from the
  same synchronized Music Core timeline.
- Keeps older audio-only ESP32 satellites on their existing lightweight path;
  unsupported presentation data is never sent to them.
- Records completed, stopped, replaced, source-failed, and transport-failed
  Sendspin outcomes so Music Core can distinguish a bad track from a speaker or
  network problem.

### Sat1 Audio Output

- Adds Automatic, Internal Speaker, AUX / Line-Out, and Internal + AUX choices
  to the Sat1 satellite settings.
- Sends the setting only to Sat1 hardware, leaving other satellite families
  unchanged.

## Updating

- macOS users already running v1.0.1 or later can install v1.3.1 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.3.1` or `latest` for the CPU image and
  `v1.3.1-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
