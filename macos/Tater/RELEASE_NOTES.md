# Tater v1.0.3

Tater v1.0.3 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Firmware Flasher

- Redesigned the manual firmware card around three clear methods: **OTA**,
  **Local USB**, and **Browser USB**. Each method now lists only compatible
  connected satellites or firmware families.
- Added Local USB factory flashing for ESP32-S3 Tater satellites from the Tater
  desktop app. macOS users can now flash Satellite1 and other supported ESP
  families without opening Chrome; Browser USB remains available.
- Added Tater-managed Espressif `esptool` support, including automatic private
  runtime installation, serial-port selection, full factory-image erase/write,
  verification, reboot, and progress reporting.
- Simplified firmware progress windows to the Tater progress interface instead
  of exposing the raw helper console.
- Added passive **USB Logs** for Local USB devices. ESP satellites use their USB
  serial/JTAG console without toggling reset lines, while the ThirdReality S420
  uses its CH340 debug-board console for boot, U-Boot, and Linux logs.

### ThirdReality S420 Recovery

- Hardened S420 Local USB recovery across Amlogic burn-mode entry, NAND-header
  capture, partition writes, post-reset payload restoration, and final Tater
  boot verification.
- Added safer macOS helper process handling and watchdogs so a stalled Amlogic
  helper fails clearly instead of leaving the firmware window hanging.
- Improved native satellite identity cleanup so forgotten or replaced devices
  do not leave stale runtime, registry, credential, or selector-alias records.

### Wake Verification Settings

- Moved **STT Wake Verification** from **Settings → Voice → Settings** to
  **Settings → Models → Wake Word**, directly below the existing Wake Word and
  Trainer Feedback settings.
- Updated **Reset Verification Stats** to clear stored and live verifier
  counters and remove disconnected satellites from the results table. Connected
  satellites remain visible with fresh zeroed results.

## Updating

- macOS users already running v1.0.1 or v1.0.2 can install v1.0.3 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.3` or `latest` for the CPU image and
  `v1.0.3-nvidia` or `nvidia` for the NVIDIA image.
