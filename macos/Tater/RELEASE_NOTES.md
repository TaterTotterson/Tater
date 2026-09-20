# Tater v1.2.2

Tater v1.2.2 is a focused Presence hotfix that makes room detection stable when
nearby satellites hear the same Bluetooth device at different times.

## What's Changed

### More Reliable Presence Rooms

- Prevents the newest Bluetooth advertisement from being mistaken for the
  closest-room signal.
- Accounts for the normal 10–30 second gaps between reports of the same device.
- Requires two separate fresh observations before confirming a room movement.
- Keeps real movement responsive while reducing false jumps for stationary
  devices.

## Updating

- macOS users already running v1.0.1 or later can install v1.2.2 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.2.2` or `latest` for the CPU image and
  `v1.2.2-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
