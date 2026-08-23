# Tater v1.1.11

Tater v1.1.11 makes Face ID a shared part of Tater's People system so camera
features can recognize the same people without depending on Awareness Core.

> **Face ID upgrade notice:** Existing faces saved by Awareness Core are not
> migrated into the new shared face directory. After updating, rebuild the
> face profiles you want Tater to recognize and link them to People again in
> Settings › People › Faces.

## What's Changed

### People and Face ID

- Adds a dedicated Faces tab under Settings › People for linking captured
  faces to People, naming unknown visitors, reviewing saved crops, moving or
  removing incorrect captures, and merging duplicate profiles.
- Adds one shared Face ID identity and matching service for Awareness Core,
  Automation Core, and future camera features.
- Lets camera automations recognize and enroll faces without requiring
  Awareness Core to be installed or configured.
- Starts the shared face directory clean instead of importing the former
  Awareness-owned profiles; existing faces must be rebuilt and relinked after
  this update.
- Keeps event identity references from the new shared directory stable when
  face profiles are merged, split, or removed.

### Music UI

- Includes the queued compact-player cleanup that removes the inactive
  timeline control and restores normal shuffle-button sizing.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.11 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.11` or `latest` for the CPU image and
  `v1.1.11-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
