# Tater v95

## What's Changed

- Added the Tater Tube Core, connecting Tater to a Tater Tube Server for recent viewing context and AI-powered movie and series recommendations in Tater Tube. Learn more at [tatertube.tv](https://tatertube.tv).
- Added an animated Tater mascot to the macOS app. The mascot shows when Tater is starting, working, using tools, finished, stopped, or needs attention, with live status and result previews.
- Made Hydra feel faster with native response streaming for chat and final answers, while keeping planning, validation, and tool-call payloads fully buffered for reliable tool execution.
- Improved llama.cpp throughput with prompt-cache reuse, stable Hydra role-to-slot affinity, concurrent server-slot requests, persistent HTTP connections, and corrected text batch sizing for unified text-and-vision servers.
- Reduced turn latency by overlapping progress and state updates with tool execution, reusing async clients and the chat-job event loop, and streaming response previews directly to the web UI.
- Kept the performance improvements provider-safe: OpenAI-compatible providers stream when supported and fall back cleanly, while Hugging Face, MLX, Spud, and existing non-streaming paths retain their prior behavior.
