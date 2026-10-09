# Tater v1.5.1

Tater v1.5.1 completes the Echo 2 (Radar) settings experience by adding its
music-reactive LED ring controls.

## What's New

### Radar Music Animations

- Adds the Music Animation selector to the settings popup for Radar
  satellites, matching the control already available for Biscuit.
- Includes No Animation, Audio Glow, Beat Pulse, Level Bars, Reactive Orbit,
  and Reactive Wave choices for ordinary and Sendspin music playback.
- Sends the selected animation in Radar's live settings payload so the change
  applies immediately and persists with that satellite's settings.
- Keeps the control limited to Biscuit and Radar. ESP32 satellites remain
  unchanged until Tater Native Firmware gains a dedicated music-animation
  setting and capability.

Radar already supports these animations in Tater Echo Firmware v2.5.0, so no
additional Radar firmware update is required.

## Updating

- macOS users already running v1.0.1 or later can install v1.5.1 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.5.1` or `latest` for the CPU image and
  `v1.5.1-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
