# Tater v1.0.1

Tater v1.0.1 is a full release for both Docker and macOS. The release tag
publishes the multi-architecture CPU image and the NVIDIA image in addition to
the macOS app.

## Important: One-Time Manual Update for macOS

This release resets Tater's macOS version numbering from `100` to `1.0.1`.
Because older Tater apps compare the first version number directly, they see
`1.0.1` as older than `100` and cannot offer this release through the built-in
updater.

Mac users must install v1.0.1 manually one time:

1. Quit Tater from its menu bar icon.
2. Download the Tater v1.0.1 DMG from the GitHub release.
3. Replace the existing Tater app in Applications and relaunch it.

Tater's settings, models, integrations, and runtime data are stored outside the
application bundle under `~/.taterassistant`, so replacing the app does not
remove them. Automatic macOS updates resume normally after v1.0.1 is installed.

Docker users are not affected by the macOS updater's old version comparison.
They can pull and redeploy the normal `v1.0.1`/`latest` CPU image or the
`v1.0.1-nvidia`/`nvidia` image as usual.

## What's Changed

### Wake Word Settings

- Moved the shared Wake Word and Trainer Feedback cards to
  **Settings → Models → Wake Word**, alongside Tater's microWakeWord model
  information and Voice Activity Detection settings.
- Kept Wake Sound and Conversation controls under **Settings → Voice →
  Settings**, where they belong with runtime behavior.
- Preserved immediate satellite-wide application of wake-word changes and the
  secure Wake Word Trainer link from its new location.

### ThirdReality S420 USB Flashing

- Tater can now find the S420's CH340 debug console, verify the connected board
  from its device-tree identity, reboot it into its short Amlogic USB-burn
  window, and begin flashing without requiring the user to time a manual power
  cycle.
- Added an S420-specific raw-NAND writer that validates the factory image and
  partition layout before erasing or writing the device.
- Factory flashing writes the boot, recovery, and system partitions in safe
  chunks and performs byte-for-byte read-back verification before reporting
  success.
- Improved progress and failure messages for missing debug cables, ambiguous
  CH340 adapters, incorrect hardware, missed burn-mode windows, and NAND write
  or verification failures.
- Both debug-board USB cables must remain connected and the S420 must remain
  powered on until Tater reports that flashing is complete.

### Tater Native Satellite Firmware 0.3.6

- Updates Voice PE, Satellite1, ReSpeaker XVF3800, and ESP32-S3-BOX-3 to the
  shared native firmware version `0.3.6`.
- Restores continuous Sat1 talker tracking during voice sessions so a moving
  user—or an initially incorrect direction estimate—can be followed and
  corrected. Sat1 switches to omni steering while its speaker is active instead
  of freezing an old microphone beam.
- Aims the sound-reactive reply animation toward the direction observed most
  often during the user's listening turn, while filtering brief direction
  jumps that should not move the reply indicator.
- Updates Sat1's bundled production XMOS firmware and automatic update target
  to `1.1.1`. The separate raw four-channel USB image remains a lab-only testing
  and calibration tool and is not installed through satellite OTA updates.

### Firmware Availability

- Tater discovers official native-satellite firmware releases from the Tater
  Native Firmware repository and shows installed/latest versions in the
  firmware UI for USB and OTA updates.
- ThirdReality S420 firmware `0.2.1` remains the current Tater-native S420
  release, with local Hey Tater detection, secure native pairing, synchronized
  music playback, captive-hotspot setup, signed OTA support, and the phantom
  microphone-start fix.
