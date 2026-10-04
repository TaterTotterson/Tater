# Tater v1.2.7

Tater v1.2.7 lets third-party Cores and Verbas explicitly opt into the shared
audio relay for synchronized satellite and AirPlay groups.

## What's Changed

- Adds `shared_group_source=True` to `play_media_url_targets` for multi-target
  HTTP audio playback. Opted-in groups use one shared source.
- Keeps the flag off by default. Existing Music Core and AirPlay routes,
  stereo pairs, and unflagged third-party playback retain their current behavior.
- Preserves the opt-in when a failed resume is retried from the beginning.

## Updating

- macOS users already running v1.0.1 or later can install v1.2.7 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.2.7` or `latest` for the CPU image and
  `v1.2.7-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
