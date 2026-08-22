# Tater v1.1.7

Tater v1.1.7 makes the shared Hugging Face model browser easier to find and
better reflects the different kinds of models it can install.

## What's Changed

### Models

- Moved Hugging Face out of the LLM-only section and made it the first tab on
  the Models screen.
- Kept model downloads organized across Transformers, llama.cpp, and MLX.
- Preserved task-aware model discovery for text, vision, video understanding,
  and audio understanding models.
- Simplified the LLM section to Settings, Manage, and Debug.
- Loads the Hugging Face browser when its Models tab becomes active without
  making an unnecessary Hub request while another Settings section is open.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.7 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.7` or `latest` for the CPU image and
  `v1.1.7-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
