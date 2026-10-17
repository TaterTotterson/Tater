# Tater v1.6.3

Tater v1.6.3 adds a capability-aware openWakeWord mode so users can run OWW on
satellites that support it while keeping the matching microWakeWord fallback on
ESP32 and other MWW-only satellites.

## What's New

### OWW Where Supported

- Adds a third Wake Detection Mode alongside microWakeWord Only and Dual Wake
  Word. OWW Where Supported enables only openWakeWord on satellites that
  advertise OWW detector selection.
- Keeps microWakeWord active on ESP32 and any other satellite that does not
  advertise OWW support. Those devices receive no unsupported OWW settings.
- Uses one matched wake bundle for custom wake words. OWW-capable satellites
  receive its openWakeWord model while MWW-only satellites receive the matching
  microWakeWord half, keeping the spoken phrase consistent in every room.

### Wake Word Settings

- Updates the Wake Word page with a clear three-choice detector selector and
  descriptions of how each choice maps to capable and MWW-only satellites.
- Preserves existing microWakeWord-only and Dual Wake behavior. Echo and
  ThirdReality satellites continue to opt into OWW through their advertised
  capabilities, while older or MWW-only firmware remains on the safe fallback.

## Updating

- macOS users already running v1.0.1 or later can install v1.6.3 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.6.3` or `latest` for the CPU image and
  `v1.6.3-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
