# Tater v3.0.5

Discover and connect Bluetooth devices through a satellite while keeping the
radio off when unused.

## What's Changed

- **Replace room presence with Bluetooth.** The new Bluetooth workspace offers
  satellite selection, explicit 15-second scans, nearby devices, and saved bonds.
  Opening settings or refreshing status never starts a scan. Automatic room,
  home/away, and presence history tracking have been removed.
- **Keep connected devices working.** Discovery expires automatically and pauses
  during voice activity. Stopping a scan leaves active GATT connections, such as
  Meshtastic, open. Bluetooth powers down after the last connection and discovery
  end; saved bonds alone do not keep it enabled.
- **Use the same proxy across satellites.** Updated Native ESP32 and ThirdReality
  firmware add Echo-compatible GATT operations and secure PIN pairing. Echo and
  standalone Linux satellites use the same on-demand discovery controls.
- **Preserve saved setup.** Existing satellite bond keys and Meshtastic transport
  settings remain intact. PINs are used only for pairing and are not saved by
  Tater. Failed unpairing retains the local device record for another attempt.
- **Update Meshtastic discovery.** Meshtastic Core 0.3.6 starts its own bounded
  scan and supports compatible satellites without Echo-specific labels.

## Companion updates

Update the connected controller and the matching satellite firmware together:

- [Home Assistant bridge 1.4.19](https://github.com/TaterTotterson/Tater-Home-Assistant-Satellites/releases/tag/v1.4.19) adds the matching Bluetooth controls in HA and Mini-Tater modes.
- [Native 0.6.7](https://github.com/TaterTotterson/Tater-Native-Firmware/releases/tag/native-0.6.7): Voice PE, Sat1, ReSpeaker XVF3800, and ESP32-S3-BOX-3.
- [Echo 5.0.3](https://github.com/TaterTotterson/Tater-Echo-Firmware/releases/tag/v5.0.3): application OTA for the existing v5 hardware baseline.
- [ThirdReality S420 0.2.27](https://github.com/TaterTotterson/Tater-ThirdReality-Voice-Firmware/releases/tag/s420-0.2.27): install the system firmware update to add BlueZ and Bleak; an application-only update cannot add these dependencies. Existing Tater S420 devices can use the signed system OTA.
- [Linux runtime v1.1.13-tater4](https://github.com/TaterTotterson/Tater-Linux-Satellite/releases/tag/v1.1.13-tater4): requires a BlueZ adapter and system D-Bus permissions.
- [Meshtastic Core 0.3.6](https://github.com/TaterTotterson/Tater_Shop/releases/tag/Meshtastic_Core_v0.3.6) is available through Tater Shop.

Old firmware must be updated to stop its previous background scanning. This
on-demand proxy does not continuously monitor broadcast-only Bluetooth sensors
or provide Bluetooth speaker audio. Hardware pairing, reconnection, and voice
coexistence testing remain to be completed.

**Full Changelog**: [v3.0.4...v3.0.5](https://github.com/TaterTotterson/Tater/compare/v3.0.4...v3.0.5)
