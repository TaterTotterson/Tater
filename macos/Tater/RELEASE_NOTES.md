# Tater v1.1.8

Tater v1.1.8 makes Music Core speaker selection readable and consistent with
the device cards used by Awareness and Automations.

## What's Changed

### Music

- Shows each Music Core speaker's real name and playback type.
- Uses compact selectable cards instead of oversized, unlabeled checkboxes.
- Lets Default Speakers and AirPlay destinations use the full settings-card
  width and flow into responsive columns.
- Supports the same `presentation: cards` and `full_width` field metadata used
  by other Tater cores.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.8 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.8` or `latest` for the CPU image and
  `v1.1.8-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
