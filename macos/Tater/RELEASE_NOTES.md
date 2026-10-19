# Tater v1.7.0

Tater v1.7.0 adds provider-aware structured tool calling across Hydra while
preserving Tater's natural-language tool protocol as a compatibility fallback.
Local and hosted models can now use the tool-call format their runtime exposes
instead of having to imitate one prompt-only syntax.

## What's New

### Native Tool Calling

- Adds shared tool schemas, response normalization, validation, and execution
  support for internal llama.cpp, MLX, and Transformers models plus external
  OpenAI-compatible and llama.cpp APIs.
- Lets capable models return native structured tool calls while preserving the
  existing natural-language format for providers or models without reliable
  tool support.
- Keeps tool results and follow-up turns consistent across providers so Hydra
  can continue multi-step work without provider-specific behavior leaking into
  cores and Verba packages.

### Hydra and Tater Shop Prompts

- Updates Hydra's checker, doer, tool-selection, and validation prompts to
  prefer the structured tools supplied by the runtime.
- Updates affected Tater Shop cores and Verba packages so their internal model
  calls use the same provider-aware path, including web research, device
  control, media, support, and administration workflows.
- Retains explicit natural-language fallback instructions where compatibility
  with older or prompt-only models is still required.

## Updating

- macOS users already running v1.0.1 or later can install v1.7.0 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.7.0` or `latest` for the CPU image and
  `v1.7.0-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
