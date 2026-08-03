# Tater v99.2

## What's Changed

### Persistent Voice Statistics

- STT Wake Verification now stores per-satellite checks, rejections,
  fail-opens, and latest results in Redis instead of losing them when Tater or
  a satellite restarts.
- Voice and per-satellite statistics now use a visible 30-day collection
  period with automatic expiration and a manual reset control.
- Added a separate reset for wake-verification statistics without clearing the
  rest of the voice history.
- Stored wake-verification results remain visible while a satellite is
  offline.

### Consistent Build Version Display

- Local command-line runs and Docker images now read the same canonical Tater
  version used by the macOS application.
- Kept the small release `Info.plist` in Docker build contexts so the sidebar
  reports the correct build version on every supported installation type.
