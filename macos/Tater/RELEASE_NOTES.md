# Tater v1.0.8

Tater v1.0.8 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Clearer Awareness History

- Awareness filter switches now use compact controls that fit naturally in the
  event-history toolbar.
- List View now presents a genuinely compact single-column event timeline with
  smaller previews and concise activity summaries.
- The layout changes are scoped to Awareness, so other core settings retain
  their existing controls and card layouts.

### More Natural Reachy Music Reactions

- Reachy now varies its gestures when songs begin instead of repeating the same
  reaction every time.
- Music reactions are coordinated with playback sessions and native satellite
  routing so they remain predictable across starts, skips, and stops.

## Updating

- macOS users already running v1.0.1 through v1.0.7 can install v1.0.8 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.8` or `latest` for the CPU image and
  `v1.0.8-nvidia` or `nvidia` for the NVIDIA image.
