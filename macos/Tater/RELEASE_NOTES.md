# Tater v1.1.1

Tater v1.1.1 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### llama.cpp Speculative Decoding

- Settings now presents one Speculative Decoding feature with selectable
  Multi-Token Prediction (MTP), DFlash, and DSpark methods.
- Each method provides a recommended maximum draft-token value and lets users
  select the matching downloaded draft or sidecar GGUF.
- Tater maps the selection to llama.cpp's native `draft-mtp`, `draft-dflash`,
  or `draft-dspark` server mode and reports the active method in runtime
  diagnostics.

### Compatibility and Safety

- Existing MTP settings and environment variables remain compatible with the
  new method selector.
- DFlash and DSpark now fail with a clear message when a required matching draft
  GGUF has not been selected.
- The Apple Silicon single-slot safeguard remains limited to affected hybrid
  MTP models and no longer applies to DFlash or DSpark.

## Updating

- macOS users already running v1.0.1 through v1.1.0 can install v1.1.1 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.1` or `latest` for the CPU image and
  `v1.1.1-nvidia` or `nvidia` for the NVIDIA image.
