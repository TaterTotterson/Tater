# Tater v1.1.17

Tater v1.1.17 makes embedded firmware downloads more reliable and restores the
fast OTA handoff experience for ThirdReality S420 satellites.

## What's Changed

### Embedded Firmware Updates

- Streams native firmware packages to disk in small chunks instead of loading
  the complete package into memory.
- Prevents large SAT1 and S420 firmware downloads from exhausting memory on a
  Raspberry Pi Zero 2 W or another memory-constrained Tater host.
- Verifies package size and SHA-256 while downloading, removes incomplete
  files after failures, and only publishes fully verified cache entries.

### Slow Appliance OTA

- Marks ThirdReality S420 and SAT1 RPi firmware tasks successful once the
  satellite accepts the signed OTA command, then lets the device download,
  install, and restart in the background.
- Avoids false completion timeouts and keeps the interface available while
  these slower appliance updates finish.
- Retains signed installation plus SAT1's device-side post-restart health check
  and automatic rollback protection.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.17 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.17` or `latest` for the CPU image and
  `v1.1.17-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
