# Tater v95

## What's Changed

- Added an animated Tater mascot to the macOS app. The mascot shows when Tater is starting, working, using tools, finished, stopped, or needs attention, with live status and result previews.
- Made Hydra feel faster with native response streaming for chat and final answers, while keeping planning, validation, and tool-call payloads fully buffered for reliable tool execution.
- Improved llama.cpp throughput with prompt-cache reuse, stable Hydra role-to-slot affinity, concurrent server-slot requests, persistent HTTP connections, and corrected text batch sizing for unified text-and-vision servers.
- Reduced turn latency by overlapping progress and state updates with tool execution, reusing async clients and the chat-job event loop, and streaming response previews directly to the web UI.
- Kept the performance improvements provider-safe: OpenAI-compatible providers stream when supported and fall back cleanly, while Hugging Face, MLX, Spud, and existing non-streaming paths retain their prior behavior.
- Fixed Tater Native satellite actions so commands, logs, status, settings, and OTA work stay on the event loop that owns their WebSocket connections.
- Renamed the settings surface from ESPHome to Tater Voice while preserving the former routes as compatibility aliases.
- Tightened continued-chat recovery so retry prompts require VAD-confirmed speech and occur at most once per conversation, reducing false responses to background noise.

## Notes

- No Tater Native satellite firmware update is required for these app-side runtime fixes.
