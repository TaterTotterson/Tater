# Tater v97.7

## What's Changed

### Satellite-Owned Timers

- Native satellites now own their timer state, countdowns, and alarms instead of relying on Tater to persist or restore them.
- Added support for multiple concurrent named timers on each satellite.
- Timer requests can start, list, cancel, snooze, or stop a timer by name, duration, or identifier.
- Reconnecting a satellite no longer clears or rebuilds its local timers.
- Tater queries live timer state from connected satellites only when it is needed.

### S3 Box Display

- Added per-S3 Box screen-brightness controls.
- Added optional scheduled night dimming with configurable dim and restore times and a separate night brightness.
- Screen settings only appear for individual S3 Boxes and are not sent to LED-only satellites.
- Tater synchronizes local time to the S3 Box so its dimming schedule can continue through temporary disconnects.

### Timer Verba

- Updated the Timer Verba for satellite-owned, concurrent named timers.
- Improved timer-name and duration parsing for requests such as “cancel the 10 minute timer” or “how much time is left on the pasta timer.”
- Timer responses now distinguish missing, ambiguous, unavailable, and successfully completed operations.

### Validation

- Added regression coverage for timer routing, named timer parsing, S3-only screen controls, schedule normalization, and firmware time synchronization.
