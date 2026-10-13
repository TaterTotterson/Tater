# Tater v1.5.4

Tater v1.5.4 adds complete Echo Show 8 (1st Gen) management, gives every
satellite one shared wake profile, and improves native screen context and voice
turn reliability.

## What's New

### Echo Show 8 (1st Gen)

- Recognizes Crown hardware as the Echo Show 8 1st Generation throughout the
  satellite list, settings, display profiles, artwork, and firmware panel.
- Offers Crown only its matching checksum-verified factory and OTA artifacts;
  capability and base-OS checks prevent an incompatible update from being
  offered or installed.
- Includes Crown in display themes, weather sensor selection, automatic audio
  settings, and the Echo output-chain controls.

### Native Screen Context

- Retains the newest matching Awareness event after its popup expires and sends
  it again when a capable screen reconnects, allowing Crown's Last Event card
  to remain useful without keeping a notification over the rest of the UI.
- Adds daylight information to the compact weather feed, preferring provider
  day/night state or sunrise and sunset data and using local time as a safe
  fallback. Native screens can therefore show night artwork instead of a bright
  sun after dark.
- Keeps each screen's selected indoor and outdoor sensor profile intact while
  adding Crown as a first-class target.

### One Wake Profile

- Replaces separate ESP and Echo wake-model forms with one wake phrase for the
  whole satellite fleet. microWakeWord runs everywhere; Dual Wake adds the
  matching openWakeWord confirmation on capable Echo firmware.
- Projects the microWakeWord half of a validated custom dual bundle to
  MWW-only satellites while sending both models to compatible Echos, so rooms
  cannot silently listen for different phrases.
- Migrates existing custom selections without discarding them and makes wake
  verification use the same shared profile that produced the device wake.

### Voice Reliability

- Stops discarding short but legitimate command transcripts based on a
  completeness heuristic. Low-signal filtering remains, while valid clipped or
  concise requests now continue through normal speaker matching and response
  handling.

## Updating

- macOS users already running v1.0.1 or later can install v1.5.4 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.5.4` or `latest` for the CPU image and
  `v1.5.4-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
