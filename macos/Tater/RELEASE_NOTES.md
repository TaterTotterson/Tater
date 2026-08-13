# Tater v1.0.11

Tater v1.0.11 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Automatic MLX Runtime Repair

- Fresh macOS installations now include the complete dependency set required by
  Tater's bundled MLX engine.
- Existing macOS installations automatically refresh their private Python
  runtime after updating, without removing models, settings, or user data.
- Setup and startup now verify a real MLX engine import so incomplete runtimes
  are repaired before the backend starts.
- MLX startup failures now identify the missing dependency and explain how to
  run the automatic repair instead of directing app users to a source script.

## Updating

- macOS users already running v1.0.1 through v1.0.10 can install v1.0.11 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.11` or `latest` for the CPU image and
  `v1.0.11-nvidia` or `nvidia` for the NVIDIA image.
