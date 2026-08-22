# Tater v1.1.9

Tater v1.1.9 keeps music playing correctly when TTS replies are delivered to a
stereo pair that is part of synchronized playback.

## What's Changed

### Voice and Music

- Recognizes a stereo pair's active music session even when that session is
  owned by a larger synchronized speaker group.
- Plays replies as synchronized TTS overlays instead of replacing the active
  music with a temporary mono session.
- Restores the music automatically after TTS finishes on both speakers.
- Limits stereo overlay completion tracking to the addressed pair so other
  members of a multi-room group do not delay the reply.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.9 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.9` or `latest` for the CPU image and
  `v1.1.9-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
