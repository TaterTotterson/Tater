# Tater v99.1

## What's Changed

### Persistent Native Satellite Pairing in Docker

- Updated both the standard and NVIDIA Docker images to store Tater Native
  satellite pairing credentials at
  `/app/.runtime/native_satellite_credentials.json`.
- Native satellite pairings now survive Docker container rebuilds and image
  updates when the documented `/app/.runtime` volume is mounted.
- Clarified the Docker persistence documentation so the runtime volume covers
  both local runtime settings and native satellite credentials.
