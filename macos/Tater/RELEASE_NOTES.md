# Tater v1.9.0

Tater v1.9.0 adds the Echo Show 8 2nd Generation to the satellite family and
introduces per-satellite display controls for every supported Echo screen.

## What's New

### Echo Show 8 2nd Generation

- Recognizes Athena and model A8H3N2 as the Echo Show 8 2nd Generation in the
  satellite list, saved offline devices, display profiles, and firmware panel.
- Uses the same rectangular Echo Show artwork as Checkers and Crown throughout
  the satellite and firmware interfaces.
- Adds Athena to Tater's verified Echo OTA path, including capability checks,
  checksummed release artifacts, staged bridge updates when required, and the
  final application A/B update with rollback protection.
- Enables Athena's display themes, photos, weather and sensor profiles, and the
  low-latency Echo Sat Chat video profile.

### Display & Night Controls

- Adds a Display & Night section to each Checkers, Crown, Athena, and Rook
  satellite popup with direct screen-brightness control.
- Adds smooth automatic brightness using the satellite's ambient-light sensor,
  with a configurable minimum level and safe manual fallback on hardware that
  does not report a usable sensor.
- Adds per-satellite overnight schedules with dim display, red bedside clock,
  or screen-off behavior, including schedules that cross midnight.
- Keeps the screen brightness setting as the automatic-mode ceiling and gives
  Echo screens conservative defaults suited to always-on room displays.

### Compatibility and Safety

- Sends the expanded screen settings only to firmware that advertises the
  screen-power-management contract, preserving compatibility with older Echo
  builds.
- Keeps OTA unavailable when a satellite explicitly reports that it cannot
  update, and improves device-name matching so Show models cannot be mistaken
  for an Echo Dot when hardware metadata is incomplete.
