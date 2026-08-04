# Tater v99.4

## What's Changed

### Portable Docker Model Runtime

- Built llama.cpp with `GGML_NATIVE=OFF` in both the standard CPU and NVIDIA
  Docker images so the bundled `llama-server` is portable across supported
  host processors instead of being optimized for the GitHub Actions runner.
- Fixed GGUF models failing to load with `llama-server` exit code `-4` on
  compatible Home Assistant and Docker hosts whose CPUs do not support every
  instruction exposed by the image build runner.
- Kept the existing AVX2 CPU optimizations and CUDA acceleration while avoiding
  host-specific instruction generation.
