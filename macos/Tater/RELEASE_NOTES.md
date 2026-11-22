# Tater v3.0.8

Satellite diagnostics now follow live network and wake status. Tater Native
Firmware 0.7.0 adds tested USB Ethernet and PoE support for SAT1 and Voice PE.

## What's Changed

- **Correct USB Ethernet diagnostics.** Read the native ESP32 network status
  as well as the existing Echo status format. SAT1 and Voice PE now show
  **Connected by USB Ethernet** when that is their active connection.
- **Refresh live satellite details.** Overlay current heartbeat data on the
  cached satellite cards during their normal refresh, rather than leaving
  network and wake readiness stale for up to five minutes. Keep cached
  discovery/settings and unsaved edits. Missing wake readiness is shown as
  unavailable; a successful cached wake-model load no longer appears as an error.
- **Native Firmware 0.7.0.** The standard SAT1 and Voice PE images now include
  Ethernet priority after DHCP, saved Wi-Fi fallback, computer USB console
  protection, and network diagnostics. Fix USB frame boundaries, mDNS hostname
  conflicts, and same-subnet routing so Tater traffic actually uses Ethernet.
  ReSpeaker XVF3800 and ESP32-S3-BOX-3 remain Wi-Fi-only in this firmware release.
- **Tested PoE adapter and setup documentation.** SAT1 and Voice PE passed
  Ethernet voice requests, full downloaded speech playback, and cold starts
  with both USB-C orientations using the powered
  [UCTRONICS B0B9ZLVGJ4 (U6113)](https://www.amazon.com/dp/B0B9ZLVGJ4), the same
  adapter tested with supported Echo satellites. For ESP32, add a **data-capable
  Micro-USB female → USB-C male converter**, use the ESP32 USB port, and cold
  boot through the adapter. The adapter supplies 5 V; SAT1's higher-voltage
  amplifier modes are not provided by this setup.
- **Adapter compatibility guidance.** Other powered RTL8152 adapters exposing
  USB `0bda:8152` and CDC-ECM configuration 2 may work with native firmware but
  are untested. Other chipsets are not enabled; a power-only splitter cannot
  carry Ethernet data. Echo compatibility follows its own drivers and board
  instructions. The README now documents both firmware families.

Firmware and app updates are separate: install the matching
[Native Firmware 0.7.0 release](https://github.com/TaterTotterson/Tater-Native-Firmware/releases/tag/native-0.7.0)
on SAT1/Voice PE and update Tater to 3.0.8 for the corrected display. Ethernet
handoffs may interrupt playback. Extended music/power soak and live failover
with power retained remain unverified on hardware; see the
[Native USB Ethernet guide](https://github.com/TaterTotterson/Tater-Native-Firmware/blob/native-0.7.0/docs/usb-ethernet.md).
For Echo power and connector differences, follow the
[Echo adapter guide](https://github.com/TaterTotterson/Tater-Echo-Firmware#usb-ethernet-adapters).

**Full Changelog**: [v3.0.7...v3.0.8](https://github.com/TaterTotterson/Tater/compare/v3.0.7...v3.0.8)
