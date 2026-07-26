# Tater v97.4

## What's Changed

### Parakeet ONNX Reliability

- Fixed first-run Parakeet model setup failing with a missing `encoder-model?int8.onnx` error.
- Tater now downloads and validates the required INT8 model snapshot before loading it through ONNX ASR.
- Complete local snapshots are reused without contacting Hugging Face, so Parakeet remains available after download when Tater is offline.
- Empty or partial model directories now resume provisioning instead of being mistaken for complete offline models.

### llama.cpp Reload Reliability

- Fixed full local llama.cpp model reloads crashing with exit `-11` on macOS.
- macOS engine workers now retain the safe `posix_spawn` launch path when started by background model warmup.
- Full model load, unload, and reload operations work without restarting Tater.
- Linux keeps dedicated process-group cleanup, with regression coverage for both platform launch paths.
