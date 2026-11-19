# Tater v3.0.3

Tater 3.0.3 adds satellite sound presets and makes the Integrations manager
consistent with the other library sections.

## What's Changed

- **Satellite sound presets:** choose Flat, Warm, Voice, or Manual in each
  satellite's Speaker Equalizer. The selector updates all eight faders together.
  Flat resets the curve, Warm adds gentle bass and softer highs, and Voice
  emphasizes speech.
- **Preserve your own tuning:** Warm is the default only when no EQ has been
  saved. Existing curves remain Manual, including flat curves. Moving a fader
  or changing Presence Boost selects Manual; choosing Flat, Warm, or Voice
  applies its complete curve and turns Presence Boost off. Reset Flat follows
  the same behavior.
- **Keep satellite EQ in sync:** with Echo firmware 5.0.1 or newer, accepted EQ
  changes made on the satellite are saved in Tater. On first connection, Tater
  adopts the satellite's restored EQ before sending settings when no host EQ
  has been saved. Existing host EQ takes priority on reconnect.
- **Preserve speaker protection:** presets leave bass protection and the peak
  limiter unchanged, including the -10 dB default on Radar, Crown, and Athena
  and the -30 dB default on Biscuit, Checkers, Cronos, and Rook. Explicit user
  overrides remain in place.
- **Integration manager tabs:** replace the View dropdown with visible
  Installed, Store, Manage, and Repositories tabs. Manage shows the number of
  available updates; search, settings, downloads, updates, and repository
  controls remain available.

Warm uses the community EQ curve suggested by @MartyTremblay in
[Echo Firmware issue #13](https://github.com/TaterTotterson/Tater-Echo-Firmware/issues/13).
Preset controls work with Echo firmware that supports the speaker output chain;
firmware 5.0.1 adds preset names and local EQ synchronization.

**Full Changelog**: [v3.0.2...v3.0.3](https://github.com/TaterTotterson/Tater/compare/v3.0.2...v3.0.3)
