# Tater v1.0.12

Tater v1.0.12 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Reliable macOS Update Startup

- The launcher now serializes dependency checks, setup, backend startup, and
  recovery so an update cannot start two backend processes concurrently.
- Recovery waits for the normal bootstrap operation to finish, preventing the
  port 8501 conflict and Python exit that could appear after updating to
  v1.0.11.

### macOS Local Network Metadata

- Tater now provides macOS with a clear explanation of why it uses the local
  network.
- The app declares the AirPlay and RAOP Bonjour services used to discover
  compatible speakers on the network.

## Updating

- macOS users already running v1.0.1 through v1.0.11 can install v1.0.12 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.12` or `latest` for the CPU image and
  `v1.0.12-nvidia` or `nvidia` for the NVIDIA image.
