# Tater v2.0.2

Tater v2.0.2 adds an optional, dedicated Decision Router for faster Hydra
routing experiments, along with focused model discovery, testing, and live
diagnostics that make router behavior easy to tune and verify.

## What's Changed

### Optional Decision Router

- Adds Off, Observe, and Active modes for a lightweight model that scores each
  request as normal chat or needing an available tool.
- Keeps tool choice and execution inside Astraeus. The router only chooses the
  path, and uncertain decisions safely fall back to the normal full Hydra flow.
- Lets high-confidence chat bypass Astraeus and sends high-confidence tool
  requests to a focused planner-only Astraeus path.
- Runs the router in its own isolated llama.cpp process and context so it does
  not consume Base or Hydra prompt-cache slots.
- Adds an adjustable confidence threshold for tuning the tradeoff between speed
  and fallback safety.

### Router Testing and Diagnostics

- Adds a built-in benchmark covering normal chat, single-tool requests,
  follow-up commands, unsupported actions, and multi-tool requests.
- Reports overall and per-category accuracy, routing coverage, precision,
  unsafe routes, latency, route score, and confidence.
- Shows recent live decisions in both Observe and Active modes, including the
  threshold and the route effect applied to each request.

### Focused Model Discovery

- Adds a Decision routing section to the model downloader for native System One
  text-classification GGUF models.
- Filters out multimodal, wrapper, and ordinary classification models that are
  not compatible with Tater's router.
- Refreshes installed model metadata after downloads and removals so compatible
  router models appear without restarting Tater.
- Leaves Tater's router recommendations empty until curated picks are published
  instead of presenting an unverified default.

### Cleaner Model Settings

- Moves Decision Router into the second LLM tab, directly after Base and before
  Beast mode.
- Keeps the router threshold, benchmark, model selection, and recent results
  together in one focused settings area.
