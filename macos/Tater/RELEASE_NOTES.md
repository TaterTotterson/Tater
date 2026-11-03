# Tater v2.0.4

Tater v2.0.4 adds complete management support for the Amazon Echo Show 5
second generation (2021), model C76N8S, codename `cronos`.

## What's Changed

### Echo Show 5 second-generation support

- Recognizes Cronos as its own Echo target instead of treating it as a generic
  screen satellite or first-generation Checkers device.
- Shows the correct device name and artwork throughout Satellites, settings,
  firmware status, Sat Chat, and display controls.
- Enables the full supported screen settings, including themes, photos,
  cameras, climate, clock format, screen power, Do Not Disturb, SSH, audio
  output, and live conversation displays.
- Uses Cronos's native video profile for Sat Chat and camera relay playback.

### Factory and OTA updates

- Selects the target-matched Cronos factory archive and verified application
  and System OTA artifacts from the shared Echo firmware manifest.
- Keeps Cronos separate from the visually similar Checkers target so an update
  for the wrong Echo Show 5 generation cannot be selected.

### Hydra prompt and tool execution

- Reduces planner prompt overhead while preserving routing, authorization,
  pending-interaction, tool-ordering, and prerequisite-discovery rules.
- Uses native structured tool schemas throughout Hydra planning, plan review,
  and step execution instead of retrying with legacy JSON-in-text prompts.
- Rejects missing or malformed native structured output rather than treating
  JSON-like response text as a valid tool decision.

### Echo Firmware v4.1.0 companion release

- Adds the first hardware-tested Tater Linux release for Cronos, including its
  factory installer, native display and audio stack, TWRP recovery, A/B OTA,
  and automatic USB Ethernet with Wi-Fi fallback.
- See the [Echo Firmware v4.1.0 release](https://github.com/TaterTotterson/Tater-Echo-Firmware/releases/tag/v4.1.0)
  for firmware downloads and complete device-side notes.

**Full Changelog**: [v2.0.3...v2.0.4](https://github.com/TaterTotterson/Tater/compare/v2.0.3...v2.0.4)
