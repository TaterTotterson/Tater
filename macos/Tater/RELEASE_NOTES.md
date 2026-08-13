# Tater v1.1.0

Tater v1.1.0 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Local Face ID

- Tater can optionally install and load a private DeepFace model from Settings
  > Models > Face ID, then unload it again when Face ID is disabled.
- Face processing stays local and automatically uses supported Apple Metal or
  NVIDIA CUDA acceleration when available, with a CPU fallback.
- The new runtime supports Awareness burst snapshots, face embeddings, known
  and unknown identity matching, and bounded reference-image storage.

### People and Awareness

- Settings > People now provides a themed, sortable directory with Face ID
  previews, linked identities, last-seen camera context, and streamlined person
  management.
- Face identities can be linked to Tater People so Awareness history and event
  search can answer when and where a known person was seen.
- Companion Tater Shop updates add the Awareness Face ID gallery and a linked-
  person trigger for Automation Core.

### Satellite Settings Reliability

- Reply Playback, Room / Area, and related satellite selections now remain
  saved after the first click instead of briefly returning to an older cached
  value.
- Stale background snapshots can no longer overwrite a newly saved satellite
  setting while the Firmware interface refreshes.

## Updating

- macOS users already running v1.0.1 through v1.0.12 can install v1.1.0 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.0` or `latest` for the CPU image and
  `v1.1.0-nvidia` or `nvidia` for the NVIDIA image.
