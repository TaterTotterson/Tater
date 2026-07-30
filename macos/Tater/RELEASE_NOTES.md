# Tater v98.2

## What's Changed

### AI Task Background Audio

- Fixed AI Task audio scenes falling back to TTS-only when loading their
  selected background track.
- Agent Lab presets and uploads are now resolved securely from local storage,
  avoiding WebUI authentication failures on Tater's loopback URL.
- Custom external background URLs continue to download on the background HTTP
  worker without blocking Tater's event loop.
- Added regression coverage for both Agent Lab and external background-audio
  sources.
