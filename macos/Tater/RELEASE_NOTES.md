# Tater v98.4

## What's Changed

### Unified Device Control

- Added one natural-language Device Control Verba for controlling and checking
  supported smart-home devices across every enabled integration.
- Device selection now understands rooms, integration-provided aliases, and
  original integration names retained after a device is renamed in Tater.
- Exact device names, aliases, IDs, and room groups are resolved
  deterministically. Every non-exact or ambiguous single-device choice must be
  selected by AI from an action-compatible candidate list.
- Candidate scoring can order choices for the AI but can no longer authorize a
  device action by itself.
- Added shared support for lights, switches, plugs, fans, covers, garage doors,
  locks, thermostats, cameras, media players, remotes, scenes, and scripts.

### Tater Shop Device Verba Change

- Removed the old category-specific control Verbas from Tater Shop, including
  the separate light, switch, plug, fan, cover, climate, camera, lock, remote,
  garage-door, scene, and script controls.
- Install and enable the new **Device Control** Verba instead.
- There is no automatic migration or fallback. Existing installations should
  disable or remove previously installed category-specific control Verbas after
  installing Device Control.

### Voice Navigation

- Moved Stereo Pairs out of the Satellites tab and into its own Voice tab.
- Voice tabs are now ordered Satellites, Firmware, Stereo Pairs, Stats, and
  Settings.
