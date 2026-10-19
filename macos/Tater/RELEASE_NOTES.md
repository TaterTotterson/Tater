# Tater v1.7.1

Tater v1.7.1 adds opt-in model thinking with separated reasoning visibility
across Hydra and chat, and fixes Hugging Face download progress so it reflects
the active transfer instead of being reset by worker heartbeats.

## What's New

### Model Thinking and Reasoning

- Adds one Model Thinking setting for internal llama.cpp, MLX, and Transformers
  runtimes plus external OpenAI-compatible and llama.cpp APIs.
- Keeps thinking off by default so users explicitly opt in for models and
  runtimes that support it.
- Preserves provider reasoning separately from visible responses, tool calls,
  and speech instead of mixing private work into the assistant's answer.

### Reasoning Visibility

- Records compact reasoning summaries and role-specific traces in the Hydra
  Ledger for diagnostics when thinking is enabled.
- Adds a collapsed thinking row to Tater chat with a short summary and an
  expandable view of the full separated reasoning trace.
- Carries structured reasoning through streaming and multi-step tool calls while
  keeping the normal chat response clean.

### Hugging Face Downloads

- Prevents downloader heartbeat events from overwriting measured file progress
  with the download phase's four-percent floor.
- Shows the active file's true transfer percentage in both the current-download
  card and model progress list.

## Updating

- macOS users already running v1.0.1 or later can install v1.7.1 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.7.1` or `latest` for the CPU image and
  `v1.7.1-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
