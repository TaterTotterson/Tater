# Tater v1.0.9

Tater v1.0.9 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### AirPlay Receiver for Tater Music

- iPhone, Apple Music, Music Assistant, and other AirPlay senders can now use
  Tater Music as one remote entry point for a selected speaker group.
- The receiver uses the same Shairport Sync input layer on macOS and Docker,
  with reliable enable, disable, restart, and speaker-group changes.
- macOS launches the receiver without a Dock icon and reloads the web UI without
  stale cached assets. CPU and NVIDIA Docker images include the pinned receiver.

### Unified Players, Volume, and Synchronization

- AirPlay input can target Tater Native satellites, discovered AirPlay devices,
  Sonos players, stereo pairs, or mixed groups through the same player routing.
- External and local Music Core playback hand off cleanly when they use the same
  speakers, while independent groups can continue playing separately.
- Sender volume now maps consistently across grouped speakers, and 100% reaches
  the full hardware volume available on Native satellites.
- Session timing, pre-roll, latency compensation, and drift correction keep
  stereo and multi-room playback aligned across supported player types.

### Refined Music Core Interface

- AirPlay has its own full-width tab, and the playlist is now the first tab next
  to Browse Library, Recommendations, Tater Tube, AirPlay, and Settings.
- The compact player is now the primary layout with volume, elapsed time, a
  cleaner live progress track, and an optically centered play control.
- Speaker selectors group Tater Native and AirPlay players by type while still
  allowing every supported player to be mixed in one playback group.
- Slimmer speaker cards and simplified settings remove obsolete sync controls
  and leave more room for player names and status.

## Updating

- macOS users already running v1.0.1 through v1.0.8 can install v1.0.9 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.9` or `latest` for the CPU image and
  `v1.0.9-nvidia` or `nvidia` for the NVIDIA image.
