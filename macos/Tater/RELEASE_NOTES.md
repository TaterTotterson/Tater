# Tater v1.6.0

Tater v1.6.0 introduces Sat Chat across Echo and Little Spud devices, a new
photo-first Echo display experience, integration-backed climate and camera
controls, and a safer staged updater for the expanded Echo firmware.

## What's New

### Sat Chat

- Adds instant, no-ringing Sat Chat between connected satellites. Echo Show 5,
  Echo Show 8, and Echo Spot devices can start and end audio/video chats, while
  Echo Dot 2 and Echo 2 devices join as audio-only participants.
- Adds the same Sat Chat directory and audio/video calling support to Little
  Spud on iPhone and Android, including capability-aware participant status.
- Prioritizes call audio and control messages while retaining only the newest
  pending video frame. Receiver-specific video profiles keep calls responsive
  across phones and the different Echo screen classes.

### New Echo Screen Experience

- Adds photo-first home screens for Checkers, Crown, and Rook with seven
  bundled landscapes, static day/night weather art, selected environmental
  readings, assistant status, and theme-aware listening, thinking, tool, and
  reply glows.
- Adds per-satellite photo uploads and simple rotation presets. Once a user
  uploads a photo, that satellite rotates only the user's collection; a single
  uploaded photo remains fixed. Rook automatically receives a centered round
  crop while Checkers and Crown use their native wide layouts.
- Adds a touch-first action menu for Sat Chat, Climate, and Cameras while
  preserving hold-anywhere intercom broadcasts. Full-screen Awareness and
  music views provide larger imagery, clearer text, reliable progress, and
  tap-to-dismiss behavior.

### Climate and Cameras

- Discovers thermostats and room-temperature sensors from Tater integrations
  instead of requiring a particular core. Users can select six room readings
  for Checkers or Crown and three for Rook, then adjust the thermostat target
  and supported heat, cool, auto, and off modes directly from the satellite.
- Adds an integration-neutral camera and doorbell directory with live views,
  stable private camera identifiers, and optional push-to-talk audio on
  integrations that support it.

### Echo Updates and Playback

- Adds resumable two-stage Echo OTA migrations for releases that introduce new
  application-slot components. Tater persists the migration through reconnects
  or restarts, continues as soon as the bridge firmware reports ready, verifies
  the final version, and reports rollback, timeout, or device errors clearly.
- Streams firmware downloads in bounded chunks and removes partial artifacts
  after failures, reducing memory use and leaving each device ready to retry.
- Updates Sendspin playback for the current protocol, shared timelines, client
  buffer requests, longer AirPlay lead time, and optional low-complexity FLAC
  transport for compatible players.

## Updating

- macOS users already running v1.0.1 or later can install v1.6.0 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.6.0` or `latest` for the CPU image and
  `v1.6.0-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
