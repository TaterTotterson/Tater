# Tater v1.9.0

Tater v1.9.0 adds model-aware wake tuning for MWW, OWW, and NWW while keeping
room-specific sensitivity and environment profiles simple on each satellite.

## What's New

### Model-Aware Wake Tuning

- Adds a polished Wake Model Overrides panel above STT wake verification.
- Shows only the controls relevant to the selected detector mode: MWW, OWW,
  NWW, MWW + OWW, or MWW + NWW.
- Supports MWW threshold and sliding-window overrides plus OWW/NWW threshold
  and patience overrides.
- Applies overrides consistently to the built-in wake word, Wake Bundle
  Catalog models, and custom wake-bundle URLs.
- Restores each bundle's validated tuning immediately when an override is
  disabled.

### Cleaner Per-Satellite Settings

- Removes model-level numeric overrides from each satellite popup.
- Keeps Wake Sensitivity and Wake Environment as room-specific controls,
  including the TV Nearby environment profile.

### Better Device Organization

- Adds an Appliances category for washers, dryers, dishwashers, and similar
  connected devices so their entities no longer fall into generic sensors.
