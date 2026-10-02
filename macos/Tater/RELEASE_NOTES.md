# Tater v1.2.5

Tater v1.2.5 gives native satellite announcements one synchronized audio
timeline and completes generated-media delivery to Tater Open WebUI.

## What's Changed

### Synchronized Announcement Scenes

- Renders an announcement's background audio, foreground speech, lead-in,
  ducking, release, and fade into one seekable MP3 before playback.
- Starts that same rendered timeline across a single satellite, stereo pair, or
  mixed multi-room group so music and speech remain aligned everywhere.
- Sends multi-destination audio scenes through one synchronized group request
  and can wait for the complete rendered scene before reporting completion.
- Keeps the existing compatibility mixer available for older satellites that
  do not support synchronized media sessions.

### Faster and More Reliable Native Audio

- Normalizes reusable background tracks with seekable Info/Xing duration
  metadata and caches prepared MP3 assets to avoid repeated transcoding.
- Preserves configured foreground delays and extends completion timeouts to
  include the scene's release and fade tail.
- Reports the exact satellite and timeout duration when a group member cannot
  prepare its media session, making playback failures easier to diagnose.

### Tater Open WebUI Generated Media

- Allows an authenticated, paired Tater Open WebUI client to retrieve the
  generated artifact URLs returned by its own Hydra calls.
- Completes the secure delivery path used to persist and display generated
  images, audio, video, and ordinary files in Tater Open WebUI chats.
- Keeps every other restricted Spud Link endpoint outside the Tater Open WebUI
  role's allowlist.

## Updating

- macOS users already running v1.0.1 or later can install v1.2.5 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.2.5` or `latest` for the CPU image and
  `v1.2.5-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
