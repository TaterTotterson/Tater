# Tater v2.0.7

Tater v2.0.7 improves speech spoken immediately after the wake word, adds
end-to-end support for trained wake-boundary models, and shows exactly which
models are included in the active wake bundle.

## What's New

### Precise wake-to-command handoff

- Accepts schema-v4 wake bundles from the updated wake-word trainers and
  validates the boundary model's type, runtime contract, size, and SHA-256
  before making it available to an Echo satellite.
- Proxies the boundary TFLite artifact alongside MWW, OWW, and NWW models, so a
  private or remote trainer URL is never exposed directly to the satellite.
- Works with MWW, OWW, NanoWakeWord CNN or GRU, MWW + OWW, and MWW + NWW. The
  small model stays resident on the Echo and is replayed only after a detector
  accepts a wake, avoiding continuous boundary-model inference.
- Uses the boundary model's acoustic wake endpoint to keep the wake phrase out
  of command speech while preserving a command spoken immediately afterward.
  Older bundles and firmware retain the existing guarded detector estimate.

### Reliable short phrases

- Treats any final STT result containing real words as actionable even when VAD
  measured a very short or quiet utterance, so phrases such as “go,” “TV,” and
  “you're cool” are no longer discarded before reaching the assistant.
- Continues dropping blank, punctuation-only, and filler-only results such as
  “um” or “uh,” preserving the existing protection against empty voice turns.
- Recovers actionable STT text from the no-speech guard without requiring it to
  pass a second duration or loudness heuristic. Clearly incomplete transcripts
  receive a direct retry prompt instead of being silently lost.

### Wake bundle visibility

- Adds a themed, read-only **Included models** panel to Wake Word settings.
  It reports whether the loaded bundle contains MWW, OWW, NanoWakeWord CNN,
  NanoWakeWord GRU, and the wake-boundary model.
- Supports the new boundary metadata in built-in, catalog, and custom bundle
  selections while keeping existing schema-v1 through schema-v3 bundles
  compatible.

### Related trainer and Echo releases

- [Wake Word Trainer v47.4 for Apple Silicon](https://github.com/TaterTotterson/microWakeWord-Trainer-AppleSilicon/releases/tag/v47.4)
  and [NVIDIA](https://github.com/TaterTotterson/microWakeWord-Trainer-Nvidia-Docker/releases/tag/v47.4)
  can train or retrain the boundary model without rebuilding selected detector
  models, then publish it inside the schema-v4 bundle with its runtime settings
  and integrity metadata.
- [Echo Firmware v4.2.0](https://github.com/TaterTotterson/Tater-Echo-Firmware/releases/tag/v4.2.0)
  installs the verified boundary artifact, keeps it loaded, replays buffered
  wake audio after acceptance, and places the command marker at the detected
  acoustic endpoint across every supported detector mode.
- The three releases remain independently compatible: Tater and Echo continue
  using the guarded timing fallback whenever a bundle has no boundary model.

### Release maintenance

- Manual CPU and NVIDIA Docker publishes can now pin a specific llama.cpp
  revision, making emergency rebuilds reproducible without changing the normal
  tagged-release defaults.

**Full Changelog**: [v2.0.6...v2.0.7](https://github.com/TaterTotterson/Tater/compare/v2.0.6...v2.0.7)
