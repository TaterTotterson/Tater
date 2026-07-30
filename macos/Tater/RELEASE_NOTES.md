# Tater v98.3

## What's Changed

### Music Core Player Support

- Added reusable multi-action controls for Core UI cards so a built-in music
  player can expose Previous, Play, Stop, and Next together.
- Added satellite media-session volume forwarding for Core-owned music
  playback.
- Music Core can use the same Tater satellites, synchronized stereo pairs, room
  preferences, and supported media-player integrations as other Tater audio
  features.
- Added authenticated Little Spud music browsing, playback controls, destination
  discovery, and a protected stream proxy for on-device listening without
  exposing provider credentials.
- Existing TTS overlays continue to duck active satellite music and restore it
  after speech finishes.
