# Tater v3.0.7

Face ID now uses one local YuNet + SFace pipeline, with simpler settings and
reference matching based on confirmed photos.

## What's Changed

- **One local face pipeline.** YuNet detects faces and landmarks; OpenCV aligns
  each face for SFace's normalized 128-dimensional embeddings. The worker uses
  the CPU DNN backend on macOS and Linux, with a larger-resolution detection
  retry when needed. Model weights are pinned and verified before loading.
- **Simpler installation and settings.** Remove the Face ID model selector and
  the DeepFace, TensorFlow, RetinaFace, and old model-specific requirements.
  The macOS launcher and Linux setup checks now use OpenCV and NumPy for Face ID.
- **Preserve existing people and captures.** Rebuild references from saved images
  in the background while retaining people, links, trust flags, images, and old
  embeddings. Show migration progress and captures needing another enrollment
  photo. Old and new embeddings are never compared.
- **Use confirmed references.** Named-person recognition uses trusted images,
  SFace's reference cutoff, and a margin between competing people. Unknown-face
  clustering stays stricter, and burst matching does not relax the SFace cutoff.
  Add regression coverage for migration, matching, image alignment, model
  integrity, and a non-writing planner for reviewing old assignments.
- **Container packaging correction.** CPU and NVIDIA builds now check for the
  YuNet + SFace OpenCV APIs instead of the retired Face ID dependencies. Remove
  the unused TensorFlow CUDA installation and validate the dependency contract
  before starting container builds. Rebuilt images retain the 3.0.7 version.

Spud Hubs and Spudlets using Face ID must run the same pipeline revision. Actual
recognition accuracy still depends on camera images, lighting, and pose; this
release does not automatically reset or delete anyone's face library.

[Face ID documentation](https://github.com/TaterTotterson/Tater/blob/main/docs/face_id.md)

**Full Changelog**: [v3.0.6...v3.0.7](https://github.com/TaterTotterson/Tater/compare/v3.0.6...v3.0.7)
