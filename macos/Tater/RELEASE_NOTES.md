# Tater v1.1.13

Tater v1.1.13 adds embedded Tater support for SAT1 and expands Spud Link so
edge installs can use the models already running on their main Tater.

## What's Changed

### Embedded Tater on SAT1

- Adds the lightweight Edge / remote-only Tater profile and SAT1 Raspberry Pi
  firmware feed needed for embedded Tater installations.
- Allows additional time for SAT1 appliance updates, reboot, and firmware
  verification before reporting a timeout.

### Spud Link Model Routing

- Lets a paired Spudlet optionally run STT, TTS, vision, audio and video
  understanding, Speaker ID, Emotion ID, and Face ID on its Spud Hub in
  addition to the existing LLM route.
- Adds a themed Spud Link model-routing panel with Auto, Spud Hub, and This
  Tater choices for each supported model family.
- Keeps wake-word detection and VAD on the edge device for responsive,
  private listening while routing the heavier work to the Hub.
- Shows routed models in runtime status as loaded on the Spud Hub without
  counting their memory against the Spudlet.

### Cores and Camera AI

- Routes shared Awareness and Automation image descriptions through the
  selected vision route.
- Lets Awareness use Hub-hosted Face ID while preserving recognized names in
  event history and notifications.

### macOS Reliability

- Prevents the stale llama.cpp cleanup from hanging Tater during startup on
  Macs with a large process list.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.13 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.13` or `latest` for the CPU image and
  `v1.1.13-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
