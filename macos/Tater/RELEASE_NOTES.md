# Tater v2.0.8

Tater v2.0.8 brings precise wake-to-command handoff to compatible Tater Native
ESP32 satellites and avoids unnecessary Echo system-image updates.

## What's New

### Wake boundaries for native satellites

- Sends the complete, verified schema-v4 wake bundle to Voice PE, Satellite1,
  ReSpeaker XVF3800, and ESP32-S3-BOX-3 devices that advertise wake-boundary
  support.
- Proxies the boundary model and detector artifacts through Tater's local
  firmware endpoint, so satellites never need direct access to a private or
  remote wake-word catalog.
- Keeps older native firmware compatible by continuing to send its projected
  MWW-only manifest when boundary support is not advertised.
- Requires a verified boundary artifact before selecting the new route;
  incomplete companion bundles retain the legacy behavior.

The small boundary model runs only after the main wake detector accepts a wake.
It finds the acoustic end of the wake phrase in buffered audio, helping the
satellite exclude “Hey Tater” from speech recognition without clipping a
command spoken immediately afterward.

### Smarter Echo system updates

- Honors each Echo release's declared minimum Tater Linux system version
  instead of treating every newer app release as requiring a full system-image
  update.
- Still offers the system update when the installed version is unknown or is
  older than the required baseline, while avoiding a large download when the
  existing system already satisfies the release.
- Shows the installed system, required minimum, and latest app versions in the
  firmware status details.

### Related native firmware

- [Tater Native Firmware 0.6.0](https://github.com/TaterTotterson/Tater-Native-Firmware/releases/tag/native-0.6.0)
  adds the boundary runtime, the newer built-in Hey Tater detector, and support
  for complete schema-v4 wake bundles on all native satellite families.

**Full Changelog**: [v2.0.7...v2.0.8](https://github.com/TaterTotterson/Tater/compare/v2.0.7...v2.0.8)
