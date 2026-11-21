# Face recognition

Tater uses one local face pipeline: **YuNet + SFace**. There is no model selection setting. YuNet finds a face and its five landmarks; OpenCV aligns the original image to SFace's template and generates a normalized 128-dimensional embedding. Detection retries at a larger resolution when the first pass finds no faces.

The worker uses OpenCV's CPU DNN backend on macOS and Linux. It does not load TensorFlow, DeepFace, or a second recognition model. Enabling Face ID downloads the pinned OpenCV Zoo ONNX weights into the Face ID model directory and verifies their SHA-256 checksums before loading.

On first use after an upgrade, Tater rebuilds references from saved capture images in the background. People, links, trust flags, images, and original embeddings are retained. Old and new embeddings are never compared. Settings shows progress and reports captures that could not be converted; those people may need another clear enrollment photo. Failed captures are retried after restarting Tater. A Spud Hub and its Spudlets must use the same pipeline revision.

Only trusted images are used to recognize a named person. Uploading an enrollment photo trusts it; captured gallery photos can be confirmed with **Trust selected**.

Named-person matching uses OpenCV's SFace reference cutoff: cosine similarity of at least 0.363, equivalent to cosine distance at most 0.637. The winning person must beat another person's match by at least 0.08 in cosine distance. Unknown-face clustering keeps a tighter 0.45 cutoff to avoid combining different people. Burst matching does not increase the SFace acceptance cutoff.

To measure performance on actual camera images, place labeled photos in one folder per person and run:

```sh
python scripts/face_id_model_bakeoff.py /path/to/labeled-faces --json-output /path/to/report.json
```

The evaluator reads images without modifying saved identities. It reports detection failures, same-person and different-person distances, and acceptance/rejection rates. Its suggested cutoff is exploratory; validate it on separate captures before changing the production cutoff.

References: [PhotoPrism's pipeline](https://docs.photoprism.app/developer-guide/vision/face-recognition/), [OpenCV's reference pipeline](https://docs.opencv.org/4.x/d0/dd4/tutorial_dnn_face.html), [YuNet weights and MIT license](https://github.com/opencv/opencv_zoo/tree/main/models/face_detection_yunet), [SFace weights and Apache 2.0 license](https://github.com/opencv/opencv_zoo/tree/main/models/face_recognition_sface).

## Reviewing old assignments

Changing the model rebuilds vectors while preserving old assignments. To repair incorrect assignments, `tater.identity.cleanup.plan_cleanup` accepts a saved identity snapshot, SFace embeddings, and explicitly confirmed reference photos. It produces a reviewable plan without writing to Redis. Only those confirmed photos become trusted; old trust flags do not count as confirmation. Rejected references cannot return to the person who rejected them. Confident matches use the named-person cutoff and margin; other usable faces are grouped conservatively, and photos that cannot be embedded remain unnamed for review. Every capture and its original vector is preserved. The plan also supplies updated event mappings and aliases. Back up the live keys and check for concurrent changes before applying a plan.
