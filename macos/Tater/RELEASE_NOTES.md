# Tater v1.3.0

Tater v1.3.0 introduces Sendspin synchronized playback, adds coordinated Dual
Wake Word controls for Echo satellites, and expands live satellite animation
settings.

## What's Changed

### Sendspin Playback

- Routes grouped Music Core playback and AirPlay input through one Sendspin
  timeline for native satellites and bridged AirPlay speakers.
- Uses Sendspin for synchronized stereo-pair replies, TTS, announcements, and
  other transient audio while preserving per-speaker volume and stereo balance.
- Adds clean handoff, cancellation, completion, buffering, resampling, and
  restart behavior so newer playback reliably releases the previous stream.
- Keeps FFmpeg as the shared media decoder and updates setup diagnostics so it
  is described as a general media dependency instead of an AirPlay-only one.

### Dual Wake Word

- Adds per-family microWakeWord, openWakeWord, and Dual Wake Word modes for
  OWW-capable Echo firmware.
- Sends both detector selections together so Echo firmware can run MWW and OWW
  concurrently and require agreement on the same audio lane.
- Supports matched custom MWW/OWW bundles through Tater's local package proxy,
  including validated manifests, metadata, and ONNX model delivery.
- Updates the built-in Hey Tater guidance for Echo firmware v2.2.0, which now
  includes the paired openWakeWord model.

### Satellite Settings

- Adds Biscuit music-reactive LED animation choices, including a No Animation
  option, and carries the selection into Sendspin playback.
- Adds No Animation choices to the other supported satellite animation modes.
- Sends complete live settings snapshots so wake, display, theme, and animation
  changes stay consistent across Echo, Tater Native, and Third Reality devices.

## Updating

- macOS users already running v1.0.1 or later can install v1.3.0 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.3.0` or `latest` for the CPU image and
  `v1.3.0-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
