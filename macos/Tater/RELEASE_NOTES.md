# Tater v96.5

## What's Changed

- Improved native llama.cpp stability for MTP models by disabling the per-slot context checkpoint snapshots that can hang hybrid and recurrent architectures, while keeping MTP speculative decoding and normal slots enabled.
- Added configurable context-checkpoint controls and runtime diagnostics through `TATER_LLAMA_CPP_CTX_CHECKPOINTS` and llama.cpp's `LLAMA_ARG_CTX_CHECKPOINTS` override.
- Timed-out native llama.cpp chats now recycle the affected engine so a stuck slot cannot continue blocking Spud Link, background work, or later requests.
- Explicitly uncapped llama.cpp requests now use Tater's configured completion limit, preventing a missing end-of-sequence token from monopolizing a local slot. Context-bounded behavior for the other local providers is unchanged.
- Added regression coverage for MTP launch arguments, bounded llama.cpp generation, and timeout recovery.
