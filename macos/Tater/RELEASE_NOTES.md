# Tater v1.2.6

Tater v1.2.6 adds first-class Echo Spot support, new display personalization,
steadier synchronized playback, and safer face recognition management.

## What's Changed

### Echo Spot and Display Satellites

- Adds the 2017 Echo Spot (Rook) to Tater's Satellites and Firmware views with
  dedicated artwork, hardware detection, install guidance, and safe OTA checks.
- Adds per-device weather sensor slots for Rook and makes cleared display slots
  stay hidden instead of silently falling back to another sensor.
- Replaces irrelevant LED controls on Echo Show and Echo Spot with five live
  color themes, including matching screen accents and reply glow.
- Supports the companion Echo firmware v2.1.0 release for Biscuit, Checkers,
  and Rook, including Rook's first public build and updated voice processing.

### Smoother Stereo and Multi-Room Audio

- Feeds native stereo pairs and mixed speaker groups from one shared stream so
  every member receives the same encoded audio and recovery timeline.
- Improves underrun rejoin behavior without repeated skips while a decoder is
  still buffering, keeping long music sessions steadier.
- Makes AirPlay 2 discovery resilient to incomplete network scans and safely
  falls back to legacy RAOP when a receiver cannot reconnect through AirPlay 2.
- Applies a conservative startup volume until the sender's real AirPlay volume
  arrives, avoiding an unexpectedly loud first moment.

### Safer Face Recognition

- Adds an on-demand, paginated face gallery with trusted, provisional, and
  unreviewed filters so large capture histories remain quick to open.
- Lets users confirm the correct reference images for a person; linked people
  are now recognized only from those trusted captures.
- Improves guarded matching across poses and supported models while keeping
  ambiguous faces separate, and allows older captures to be moved or removed.

### Cleaner Satellite Settings

- Makes acoustic echo cancellation a firmware-managed feature and removes the
  old experimental manual AEC controls from every satellite family.
- Keeps Echo display weather and sensor choices tied to the specific device,
  including a condition-only layout when no sensor slots are selected.

## Updating

- macOS users already running v1.0.1 or later can install v1.2.6 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.2.6` or `latest` for the CPU image and
  `v1.2.6-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
