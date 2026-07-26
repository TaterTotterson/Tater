# Tater v97.3

## What's Changed

### macOS Update Reliability

- Fixed the native macOS updater occasionally remaining on Installing Update after the updater helper had already completed.
- Tater now detaches finished process-output streams before waiting for shutdown, preventing an end-of-file callback loop from holding the old app open.
- Added regression coverage for clean backend shutdown and updater handoff.

### Runtime Dependency Updates

- The macOS launcher now fingerprints Tater's Python requirements and automatically refreshes its private environment when an update adds or changes dependencies.
- Existing macOS installations now install the `onnx-asr` package required by Parakeet ONNX instead of retaining an incomplete older environment.
- Added a direct Parakeet dependency readiness check so a damaged or incomplete environment is repaired before Tater starts.

### Voice and llama.cpp Stability

- Added a dedicated **Save Voice Models** action that saves and warms STT/TTS without unloading or restarting an unchanged local LLM.
- Prevented a Parakeet settings change from unnecessarily recycling a working Qwen model.
- Hardened native llama.cpp shutdown on macOS by bypassing the unstable Metal finalizer that could abort inside `ggml_metal_rsets_free`.
- The llama.cpp worker now gets time to finish acknowledged cleanup before Tater signals its process group.
