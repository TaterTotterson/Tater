# Tater v3.0.11

## What's Changed

- **Stop cancelled model requests:** Cancel the matching local llama.cpp task when an API caller disconnects. Dashboard Studio cancellation and timeouts now stop abandoned generation instead of leaving the GPU working on its response.
- **Cancel during prompt processing and tool output:** Close the native model request promptly, keep the shared model loaded, and let other requests continue normally.

For Dashboard Studio's **10-minute design window** and **Cancel** button, also update the [Home Assistant bridge to 1.4.35 or newer](https://github.com/TaterTotterson/Tater-Home-Assistant-Satellites/releases/tag/v1.4.35).

**Full Changelog**: [v3.0.10...v3.0.11](https://github.com/TaterTotterson/Tater/compare/v3.0.10...v3.0.11)
