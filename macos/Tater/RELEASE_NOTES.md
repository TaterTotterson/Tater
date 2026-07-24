# Tater v96.6

## What's Changed

- Added an Apple Silicon safeguard for Qwen 3.5/3.6 35B-A3B MTP models: MTP remains enabled, but affected models use one effective llama.cpp slot to avoid the Metal parallel-slot stall.
- Added a 120-second inactivity watchdog backed by llama.cpp prompt-progress streaming. Long requests may continue normally while they are making progress; only stalled requests are recycled early.
- Native engine workers and their llama-server children now share a dedicated process group, ensuring forced recycling and parent shutdown cannot leave orphan llama-server processes behind.
- Worker shutdown now stops llama-server before joining in-flight chat threads so blocked HTTP requests unwind promptly.
- Added regression coverage for model-specific MTP launch behavior, progress-aware timeout handling, and process-tree cleanup.
