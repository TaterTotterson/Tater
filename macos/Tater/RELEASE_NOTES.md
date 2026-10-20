# Tater v1.8.0

Tater v1.8.0 adds NanoWakeWord throughout the wake-word stack, introduces one
portable wake bundle for every supported detector, and makes Hydra follow-up
actions and clarifying conversations more reliable.

## What's New

### MWW, OWW, and NWW Wake Modes

- Reorganizes Wake Detection Mode into clear single-engine MWW, OWW, and NWW
  choices plus MWW + OWW and MWW + NWW confirmation modes.
- Adds NanoWakeWord CNN and GRU selection, enabling only architectures that
  are actually included in the selected package.
- Keeps ESP32 and older satellites safely on the bundle's MWW model when they
  do not advertise support for the selected companion engine.
- Restricts OWW catalog packages to V7 or newer and NWW catalog packages to
  verified V8-or-newer bundles.

### One Wake Bundle Everywhere

- Supports the trainer's schema-v3 wake bundle as the canonical source for
  MWW, OWW, NWW-CNN, and NWW-GRU models, tuning, revisions, and integrity
  metadata while retaining the simple ESPHome MWW JSON.
- Validates the models required by the selected wake policy before applying it
  and securely proxies embedded MWW and NWW artifacts to satellites.
- Updates the built-in Hey Tater package so capable satellites can use MWW,
  OWW, NWW-CNN, NWW-GRU, or either supported dual-engine policy from one
  matched model family.

### Satellite and Trainer Compatibility

- Tater Echo Firmware v2.7.0 and ThirdReality S420 firmware 0.2.24 add native
  NWW-CNN and NWW-GRU inference, custom wake-bundle loading, architecture
  selection, and MWW + NWW confirmation.
- The Apple Silicon and NVIDIA Wake Word Trainers can train MWW, OWW, NWW-CNN,
  and NWW-GRU individually or together using the shared TTS corpus plus user
  positive and negative samples.
- Trainer output now publishes one canonical Tater wake bundle, keeps the
  ESPHome MWW package separate, and supports V8 catalog publishing for models
  that include NanoWakeWord.

### Smarter Hydra Follow-ups

- Adds short-lived, scope-aware pending interaction state so Hydra can
  distinguish confirmations, rejections, supplied details, modifications,
  unrelated chat, and genuine clarification requests.
- Uses the current user message as the only authority for continuing a pending
  action and clears stale offers instead of turning casual conversation into
  work the user did not request.
- Records the selected turn branch in Hydra diagnostics and improves direct
  clarification wording when an executable action still needs information.

### Settings Reliability

- Normalizes LLM port values across Hydra roles and Tater Open WebUI settings.
- Shows useful field-level API validation messages instead of a generic save
  error when settings contain invalid values.
