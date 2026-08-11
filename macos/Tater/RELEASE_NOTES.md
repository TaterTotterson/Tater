# Tater v1.0.5

Tater v1.0.5 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Verified Native Satellite OTA

- Native satellite OTA updates are no longer reported as successful when the
  satellite merely accepts the update and starts recovery. Tater now waits for
  the satellite to disconnect, reboot, reconnect, and report the exact expected
  firmware version before completing the progress window.
- OTA progress remains below 100% until that post-reboot verification succeeds.
  A recovery error, unexpected firmware version, or verification timeout is now
  shown as a failure instead of a false success.
- Tater starts each OTA check from a fresh device-log cursor so messages left by
  an earlier update cannot complete a new update incorrectly.

### ThirdReality S420 Firmware

- Tater S420 firmware `0.2.3` corrected the signed recovery-installer handoff so
  OTA updates actually write the downloaded firmware instead of stopping after
  the initial reboot request.
- Tater S420 firmware `0.2.4` preserves Wi-Fi, Tater pairing, room assignment,
  and device settings during routine OTA updates. Routine OTA now updates only
  the system components it needs and keeps persistent device data and the
  bootloader environment intact.
- S420 factory flashing through Local USB remains a clean reset by design. S420
  units still on firmware `0.2.2` or earlier need one Local USB update to the
  latest firmware before subsequent OTA updates can use the corrected installer.

## Updating

- macOS users already running v1.0.1 through v1.0.4 can install v1.0.5 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.5` or `latest` for the CPU image and
  `v1.0.5-nvidia` or `nvidia` for the NVIDIA image.
