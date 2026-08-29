# Tater v1.1.14

Tater v1.1.14 adds first-class SAT1 Raspberry Pi and Tater Embedded support,
then makes Spud Link and satellite firmware management much easier to
understand and use.

## What's Changed

### SAT1 Raspberry Pi and Tater Embedded

- Adds the complete Tater firmware feed for the new
  [Tater SAT1 Raspberry Pi](https://github.com/TaterTotterson/Tater-SAT1-RPi)
  project, including both Tater Embedded and satellite-only images.
- Lets Tater match connected SAT1 Raspberry Pi devices to their signed
  appliance updates in the Firmware panel.
- Keeps an embedded SAT1 update visible while its local Tater shuts down and
  restarts, then reconnects automatically and reports either a successful
  install or an automatic rollback.
- Keeps the new self-update recovery limited to a local Tater Embedded SAT1;
  existing ThirdReality, Reachy, ESP32 SAT1, and remote SAT1 update paths keep
  their current behavior.

### Why Tater Embedded Is Useful

- A SAT1 can now be a self-contained Tater for a room, area, or person instead
  of only being a microphone and speaker for one central server.
- Each embedded Tater can keep its own wake word, wake sound, memory, personal
  information, plugins, music preferences, and music recommendations.
- One or two additional satellites can connect to that room's embedded Tater,
  allowing a home to use separate satellite groups with different identities
  and behavior while still keeping a larger main Tater elsewhere.
- Wake-word detection and VAD stay on the SAT1 for responsive listening, while
  heavier speech, vision, and language-model work can use a Spud Hub or remote
  APIs.

### Spud Link and Model Routing

- Separates Little Spud QR pairing and Spudlet short-code pairing into clear,
  themed flows that confirm success automatically.
- Places each Little Spud's LAN and optional Tater Tunnel address beside its
  QR pairing action so the correct connection details are captured first.
- Organizes Spudlet connection, model routing, role, privacy, and technical
  settings into focused tabs with clearer explanations.
- Greys out model controls that are owned by the Spud Hub and labels routed
  STT, TTS, Beast Mode, Face ID, and other model families consistently.
- Shows the actual models loaded on the Hub—including shared models used for
  multiple jobs—instead of placeholder `base` entries or stale local choices.
- Improves Hub-routed Face ID and preserves authenticated person identity when
  results return to the Spudlet.

### Firmware Flasher

- Adds Factory and Keep Settings choices to both Local USB and Browser USB
  flashing for supported ESP satellites.
- Keep Settings writes the OTA application image without erasing Wi-Fi,
  pairing, or satellite settings; Factory remains available for recovery and
  first-time setup.
- Removes Tater's extra HTTPS-only gate from Browser USB and leaves browser
  capability detection to Chrome.
- Updates the SAT1 Raspberry Pi feed and local development paths for the new
  `Tater-SAT1-RPi` repository name.

### Runtime and Core Reliability

- Makes the runtime model panel distinguish remote Hub models from local
  memory use and present managed model state more clearly.
- Makes **Delete data** remove audited Core-owned Redis keys and namespaces
  without allowing downloaded Cores to choose arbitrary deletion patterns.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.14 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.14` or `latest` for the CPU image and
  `v1.1.14-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
