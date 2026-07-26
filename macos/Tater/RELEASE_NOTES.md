# Tater v97.2

## What's Changed

### STT Wake Verification

- Wake verification now uses the STT engine selected in Model Settings instead of depending on MLX Whisper.
- Added verifier support for Faster Whisper, MLX Whisper, Parakeet ONNX, Vosk, and one-shot Wyoming STT.
- Added the configured and effective STT engine to verifier status, satellite results, and runtime logs so backend fallbacks are visible.
- Preserved fail-open behavior when the selected STT service is unavailable, errors, or exceeds the verifier deadline.

### Parakeet ONNX

- Added Parakeet TDT 0.6B v3 ONNX as a selectable local multilingual STT engine with punctuation, capitalization, and automatic language detection.
- The approximately 670 MB INT8 model downloads once when selected, is stored with Tater's local models, and warms immediately for use.
- Added automatic ONNX provider selection for CUDA, ROCm/MIGraphX, Core ML, and CPU with safe fallback when the preferred accelerator is unavailable.
- Improved voice-model switching by releasing obsolete STT model caches and queuing the latest selection when another model warmup is already running.
- Added Parakeet runtime model and provider diagnostics plus focused transcription, routing, wake-verifier, fallback, and cache-management coverage.

### Docker

- Added Parakeet ONNX and CPU provider validation to the standard Tater image.
- Added the CUDA-enabled ONNX Runtime package and provider validation to the NVIDIA image while retaining CPU fallback.
