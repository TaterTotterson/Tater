# Tater v1.1.26

Tater v1.1.26 adds complete slider support to settings popups opened by cores.

## What's Changed

### Core Popup Sliders

- Renders core popup fields declared with `"type": "range"` as sliders instead
  of text inputs.
- Shows the selected value live while the slider moves, including an optional
  suffix such as `%`.
- Honors each field's minimum, maximum, and step values.
- Sends the selected value back to the core as a number when settings are saved.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.26 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.26` or `latest` for the CPU image and
  `v1.1.26-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
