# Tater v1.1.22

Tater v1.1.22 adds AdaFace as a selectable Face ID model while preserving
FaceNet compatibility, safe rollback, and centralized Spud Hub processing.

## What's Changed

### AdaFace Face Recognition

- Adds the official AdaFace IR-50 WebFace4M model as an experimental Face ID
  option alongside FaceNet512.
- Adds a recognition-model selector and migration progress to Settings ›
  Models › Face ID.
- Pins the AdaFace checkpoint revision and validates its required runtime
  dependencies across macOS, CPU Docker, NVIDIA Docker, and private setup.
- Uses RetinaFace alignment and normalized 512-dimensional, model-tagged
  embeddings with a conservative AdaFace matching threshold.
- Adds a labeled-image bakeoff utility for comparing genuine and impostor
  distances between FaceNet and AdaFace using real camera images.

### Safe Model Switching

- Re-embeds saved face crops in the background before activating a newly
  selected model.
- Keeps separate FaceNet and AdaFace embedding profiles and never compares
  vectors produced by different models.
- Preserves the previous model's embeddings for immediate rollback and only
  generates embeddings that are missing when switching again.
- Leaves the current model active and reports an error if every linked person
  cannot receive a usable embedding from the requested model.

### SpudLink Face ID Synchronization

- Lets Spudlets detect the Spud Hub's active Face ID model from tagged
  embedding responses.
- Automatically re-embeds saved crops for linked people through the Hub when
  the Hub changes models, then resumes recognition with compatible vectors.
- Keeps Face ID model execution and downloads on the Hub; connected Spudlets
  store only the returned embeddings and can retain FaceNet as a local
  fallback.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.22 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.22` or `latest` for the CPU image and
  `v1.1.22-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
