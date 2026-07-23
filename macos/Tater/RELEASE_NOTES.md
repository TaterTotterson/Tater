# Tater v96.4

## What's Changed

- Added context-bounded generation for local Transformers, llama.cpp, and MLX models, allowing explicitly uncapped calls to continue until EOS or the available model context is exhausted.
- Fixed Spud Link and direct API routing so an explicit uncapped request survives every hop instead of silently falling back to the 1,024-token default.
- Preserved the existing default completion limit for callers that do not explicitly request context-bounded generation.
- Added regression coverage for provider handling, Spud Link relay behavior, and context-aware token budgets.
