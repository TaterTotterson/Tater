# Tater v1.1.2

Tater v1.1.2 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Hugging Face Model Browser

- The selected model's download options now remain visible in a sticky details
  panel while users scroll through long model lists on desktop.
- The details panel gets its own bounded scrolling area when needed and returns
  to the normal document flow on smaller screens.
- Parameter labels now preserve meaningful trailing zeroes, so models such as
  Muse Glimmer 30B are displayed as `30B` instead of `3B`.

### New and Updated Tater Picks

- **Qwen3.8-27B Tater NoThink** adds Qwen's new dense vision-language model with
  a Q4_K_M main GGUF, matching vision projector, and native MTP draft model.
- **NVIDIA Nemotron 3.5 Lightning 30B-A3B Tater NoThink** adds the lightweight
  active-parameter MoE with its native MTP draft model.
- **Muse Glimmer 30B Tater LowThink** adds Meta's vision-capable model with all
  reasoning-effort selections redirected to low thinking and a DFlash option.
- **Gemma 4 26B-A4B Tater NoThink** now offers both MTP and DFlash draft models
  alongside its matching vision projector.
- These draft models work with the MTP and DFlash choices in Tater's unified
  Speculative Decoding settings.

## Updating

- macOS users already running v1.0.1 through v1.1.1 can install v1.1.2 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.2` or `latest` for the CPU image and
  `v1.1.2-nvidia` or `nvidia` for the NVIDIA image.
