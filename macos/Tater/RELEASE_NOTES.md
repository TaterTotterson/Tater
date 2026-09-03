# Tater v1.1.25

Tater v1.1.25 makes integration choices clearer by displaying declared select
fields as labeled dropdown menus instead of free-text inputs.

## What's Changed

### Integration Settings Dropdowns

- Displays Home Assistant's device visibility choice as a dropdown with clear
  labels for all entities or only entities exposed to Assist.
- Adds proper rendering for every integration that declares a `select` field,
  including integrations that use either labeled choices or simple text choices.
- Updates both the current Integrations screen and its legacy fallback so the
  experience stays consistent across installations.
- Preserves existing saved values and continues sending the selected underlying
  value to the integration without changing its settings format.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.25 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.25` or `latest` for the CPU image and
  `v1.1.25-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
