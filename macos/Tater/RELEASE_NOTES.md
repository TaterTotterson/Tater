# Tater v2.0.6

Tater v2.0.6 keeps the built-in model responsive while background intelligence
work is running, improves SpudLink reliability, and makes Voice Core honor the
configured conversation-history limits.

## What's New

### Responsive built-in model scheduling

- Separates live interactive requests from background memory, summary,
  discovery, and cleanup work when using Tater's built-in llama.cpp engine.
- Runs background model work only while the engine is idle and immediately
  cancels active background inference when a live request needs the GPU.
- Closes the active llama.cpp response stream during cancellation so prompt or
  generation work releases the engine instead of continuing behind the queue.
- Applies the same policy to regular Tater requests and requests received from
  a connected Spudlet.

### More reliable SpudLink replies

- Carries request activity and interactive/background priority from a Spudlet
  to the main Tater instead of treating every remote model call the same.
- Allows interactive SpudLink requests to wait longer for the built-in model
  while quickly dropping background work that cannot start during an idle
  window.
- Replaces the unrelated generic chat response after a SpudLink model failure
  with a clear message that the main Tater's model could not be reached.

### Configurable Voice Core history

- Uses the shared conversation settings for both retained Voice Core history
  and the history sent to the model instead of always using fixed 20-message
  storage and 8-message prompt limits.
- Preserves the shared, user-aware satellite conversation and its two-hour
  inactivity timeout while allowing installations to choose larger or smaller
  context windows.

**Full Changelog**: [v2.0.5...v2.0.6](https://github.com/TaterTotterson/Tater/compare/v2.0.5...v2.0.6)
