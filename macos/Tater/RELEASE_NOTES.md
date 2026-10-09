# Tater v1.5.0

Tater v1.5.0 adds experimental Echo 2 (2017, `radar`) satellite support and
speaker tuning controls for compatible Echo devices.

## What's Changed

### Echo 2 (Radar)

- Adds the 2017 full-size Echo 2 as a recognized native satellite, with the
  correct name, dedicated transparent device artwork, status display, and
  settings integration throughout the Satellites page.
- Adds Radar factory and OTA release discovery from Tater Echo Firmware,
  including the correct recovery guide and capability-aware eligibility for
  devices that first need a USB/recovery installation.
- Supports normal in-app Radar OTA updates after Tater Echo Firmware v2.5.0 is
  installed. The initial experimental factory installation still follows the
  hardware-specific recovery procedure documented by the firmware project.

### Echo Speaker Tuning

- Adds an eight-band equalizer to the settings popup for Biscuit, Radar,
  Checkers, and Rook when the connected firmware advertises output-chain
  support.
- Adds optional speech presence boost, bass protection, and peak-limiter
  controls with safe defaults and per-satellite persistence.
- Keeps processing ownership explicit: Tater sends the selected settings while
  the Echo firmware applies them once at the final hardware output.

### Native Satellite Reliability

- Accepts both map- and list-form capability announcements from native
  satellites and only exposes controls that the connected firmware supports.
- Preserves the early authenticated hello acknowledgement while negotiating
  output-chain support, avoiding setup and recovery watchdog timeouts.

## Updating

- macOS users already running v1.0.1 or later can install v1.5.0 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.5.0` or `latest` for the CPU image and
  `v1.5.0-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
