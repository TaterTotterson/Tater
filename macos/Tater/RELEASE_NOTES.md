# Tater v1.1.24

Tater v1.1.24 adds integration-owned device visibility controls and lets the
Home Assistant integration follow the entities a user exposes to Assist.

## What's Changed

### Integration-Owned Visibility

- Adds a reusable runtime filter hook that lets any integration approve or
  reject live device states and events before Tater stores or publishes them.
- Supports synchronous and asynchronous integration filters so providers can
  use local rules or refresh visibility from their own APIs.
- Preserves existing behavior for every current integration that does not
  implement a filter.
- Blocks an item and reports the error if an enabled filter fails instead of
  silently exposing provider data.

### Home Assistant Assist Exposure

- Adds a Home Assistant setting for either all entities or only entities
  exposed through Home Assistant's Assist voice pipeline.
- Honors explicit Assist exposure choices, Home Assistant's expose-new-entities
  preference, and its default domain and device-class exposure rules.
- Applies the same policy to the device catalog and live state-change events so
  filtered entities cannot reappear through the background listener.
- Clears old Home Assistant runtime states when the visibility mode changes and
  refreshes the exposure policy in the background.
- Keeps all entities as the default for existing installations and verifies
  that Assist-only mode has the required Home Assistant administrator access.
- Releases the downloadable Home Assistant integration as version 1.5.0.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.24 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.24` or `latest` for the CPU image and
  `v1.1.24-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
