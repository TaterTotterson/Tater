# Tater v3.0.4

Change satellite names and rooms live from Tater, without resetting or pairing
the device again.

## What's Changed

- **Edit names and rooms in Voice settings.** Open a connected satellite's
  **Room & audio** section, change **Satellite name** or **Room / Area**, and
  save. Clear the room field to remove its assignment.
- **Save on the satellite.** Firmware confirms that both values were saved
  before Tater applies the change. Reconnects and restarts retain the new
  identity; failed writes report an error and preserve the last confirmed values.
- **Refresh room context live.** Display targets, room sensors, and satellite
  metadata follow confirmed changes, including edits made from another controller.
  Pairing credentials, stable device IDs, and stereo-group membership stay intact.
- **Show firmware requirements.** Name and room editing is available for connected
  satellites that advertise support. Older firmware shows an update message.

## Companion satellite updates

Update Tater and install the matching firmware to enable live edits:

- [Native 0.6.6](https://github.com/TaterTotterson/Tater-Native-Firmware/releases/tag/native-0.6.6): Voice PE, Sat1, ReSpeaker XVF3800, and ESP32-S3-BOX-3.
- [Echo 5.0.2](https://github.com/TaterTotterson/Tater-Echo-Firmware/releases/tag/v5.0.2): all supported Echo satellites.
- [ThirdReality S420 0.2.26](https://github.com/TaterTotterson/Tater-ThirdReality-Voice-Firmware/releases/tag/s420-0.2.26).
- Standalone Linux satellites: shared runtime `v1.1.13-tater3`.

The [Home Assistant bridge 1.4.18](https://github.com/TaterTotterson/Tater-Home-Assistant-Satellites/releases/tag/v1.4.18)
adds the same confirmed edits from its panel and Home Assistant's device editor.
Use the satellite's connected controller; no reset or re-pairing is required.
Echo devices must already be on the v5 hardware baseline to use application OTA.

**Full Changelog**: [v3.0.3...v3.0.4](https://github.com/TaterTotterson/Tater/compare/v3.0.3...v3.0.4)
