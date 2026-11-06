# Tater v2.0.5

Tater v2.0.5 keeps voice conversations coherent across satellites, prevents
Pocket TTS setup failures before they reach playback, and improves the default
bass protection on larger Echo speakers.

## What's New

### Shared, user-aware voice conversations

- Keeps one rolling Voice Core conversation across every satellite and wake
  activation instead of starting without context after each wake word.
- Labels historical turns with the recognized person and room, allowing one
  household member to confirm or modify another member's preceding request.
- Gives the shared context to the Decision model, Astraeus, and Hermes so short
  follow-ups such as “yes,” “then do it,” and “make that three” resolve against
  the preceding conversation.
- Retains the existing two-hour inactivity timeout and per-conversation storage
  compatibility.
- Keeps action-shaped and contextual requests in the full planning path when a
  high-confidence chat route would otherwise discard the action.

### Pocket TTS access check

- Checks gated Pocket TTS voice-cloning model access before saving and loading
  Pocket TTS as a reply or announcement backend.
- Shows the exact remaining setup steps when access is blocked: install and
  enable the Hugging Face integration, add the account token, and accept the
  Pocket TTS model terms with that account.
- Rechecks access on every save until the model weights can be downloaded, then
  stops interrupting normal Pocket TTS saves.

### Better bass defaults for larger Echo speakers

- Defaults the bass-protection floor to -10 dB on Radar, Crown, and Athena,
  preserving more low-frequency output on Echo 2nd generation and Echo Show 8
  speakers. Smaller Dot, Show 5, and Spot models retain the -30 dB default.
- Preserves explicit per-satellite settings and the board-aware default during
  partial or offline settings saves.
- Recognizes Athena's `A8H3N2` hardware-model alias when selecting defaults.

### Project policy documentation

- Documents the Tater trademark policy and clarifies the relationship between
  the project's AGPL software license and use of the Tater name and logo.

**Full Changelog**: [v2.0.4...v2.0.5](https://github.com/TaterTotterson/Tater/compare/v2.0.4...v2.0.5)
