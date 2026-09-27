# Tater v1.2.3

Tater v1.2.3 adds native Amazon Echo satellites, a richer display experience,
steadier room presence, and reliability improvements throughout Tater.

## What's Changed

### New Echo Satellites

- Adds first-class support for the Echo Dot 2nd Gen and Echo Show 5 1st Gen,
  including pairing, settings, artwork, stereo playback, and OTA updates.
- Gives Echo Show users weather and room sensors, camera notifications, visual
  voice states, and on-screen intercom.
- Releases the companion [Tater Echo Firmware](https://github.com/TaterTotterson/Tater-Echo-Firmware)
  repository for factory installs and satellite updates.

### Better Presence and Audio

- Adds calibrated BLE identities, live room history, home/away tracking, and a
  dedicated Presence interface and API.
- Keeps devices steady when multiple satellites in the same room hear them,
  while still responding quickly when a device really moves.
- Improves stereo reply playback, live wake-word changes, wake sounds, and
  satellite voice feedback.

### More Reliable Day to Day

- Hardens Redis recovery and cleanup so interrupted processes do not leave
  Tater or its satellites offline.
- Improves face identity, integration state cleanup, sensor events, weather
  displays, and repeated announcement playback.

## Updating

- macOS users already running v1.0.1 or later can install v1.2.3 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.2.3` or `latest` for the CPU image and
  `v1.2.3-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
