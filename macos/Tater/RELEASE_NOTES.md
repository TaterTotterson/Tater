# Tater v3.0.10

## What's Changed

- **Faster startup:** Fixed setup running on every Mac launch after the emotion model update. Tater now keeps the model's required dependencies and skips setup when the runtime is ready.
- **Safer satellite pairing:** Pairing codes expire after five different incorrect guesses. Repeated reconnects with the same mistyped code count once. Saved satellite credentials and interrupted setup recovery continue working.
- **Clearer satellite documentation:** Added a guide to pairing limits and existing API access settings, plus SAT1 Raspberry Pi installation and embedded Tater links.

[Satellite pairing and API access](https://github.com/TaterTotterson/Tater/blob/v3.0.10/docs/native_satellite_access.md)

**Full Changelog**: [v3.0.9...v3.0.10](https://github.com/TaterTotterson/Tater/compare/v3.0.9...v3.0.10)
