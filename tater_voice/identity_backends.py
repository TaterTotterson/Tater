"""Pinned inference backends for voice identity and vocal emotion.

The speaker checkpoint includes the trained encoder, layer adapters and
attentive statistics pooling head. Loading only facebook/w2v-bert-2.0 would
produce general speech features, not these speaker embeddings.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import shutil
from pathlib import Path
from typing import Any

SPEAKER_MODEL = "zl389/w2v-bert-2.0_SV"
SPEAKER_REVISION = "4e7b92e88f69b6a0aa5a6fdd3dfdb458ed615967"
SPEAKER_CHECKPOINT = "model_lmft_0.14.pth"
SPEAKER_SHA256 = "454398fe4010bb7d1095517f3aff683c90a1eed644f68f83272c38ea81f953f9"
SPEAKER_PROVIDER = "w2v_bert2_sv"
FRONTEND_MODEL = "facebook/w2v-bert-2.0"
FRONTEND_REVISION = "da985ba0987f70aaeb84a80f2851cfac8c697a7b"
EMOTION_MODEL = "emotion2vec/emotion2vec_plus_base"
EMOTION_REVISION = "b318240bfe67db81a8c572ecb37ce9c3759b81c9"
EMOTION_SHA256 = "60710b5aae1dbe69bdac8920028fb05882d4314fd09031922b4b61ee9e7aadbd"
EMOTION_PROVIDER = "emotion2vec"
EMOTION_LABELS = ("angry", "disgusted", "fearful", "happy", "neutral", "sad", "surprised")


def retire_legacy_cache(root: Path, kind: str) -> None:
    """Retire only known old managed weights, after the replacement loads."""
    names = (("speechbrain-spkrec-ecapa-voxceleb",) if kind == "speaker_id" else
             ("speechbrain-emotion-recognition-wav2vec2-iemocap",))
    for name in names:
        old = root / name
        if old.is_symlink():
            old.unlink()
        elif old.is_dir():
            shutil.rmtree(old)


def dependencies(kind: str) -> tuple[bool, str]:
    packages = ("torch", "transformers", "huggingface_hub", "numpy", "scipy")
    if kind == "emotion_id":
        packages += ("funasr",)
    missing = [name for name in packages if importlib.util.find_spec(name) is None]
    return (False, "Missing voice model packages: " + ", ".join(missing)) if missing else (True, "")


def pcm_waveform(audio: bytes, audio_format: dict[str, Any]) -> Any:
    """Decode PCM and resample without audioop (also works on Python 3.13)."""
    import numpy as np
    from scipy.signal import resample_poly

    rate = int(audio_format.get("rate") or 16000)
    width = int(audio_format.get("width") or 2)
    channels = int(audio_format.get("channels") or 1)
    if rate <= 0 or channels <= 0 or width != 2:
        raise ValueError("Voice models require 16-bit PCM with a positive sample rate and channel count.")
    if not audio or len(audio) % (width * channels):
        raise ValueError("Voice model audio is empty or contains an incomplete PCM frame.")
    waveform = np.frombuffer(audio, dtype="<i2").astype(np.float32).reshape(-1, channels).mean(axis=1)
    waveform /= 32768.0
    if rate != 16000:
        divisor = math.gcd(rate, 16000)
        waveform = resample_poly(waveform, 16000 // divisor, rate // divisor).astype(np.float32)
    return np.ascontiguousarray(waveform)


def _files(repo: str, revision: str, names: tuple[str, ...], directory: Path, token: str) -> None:
    from huggingface_hub import hf_hub_download

    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        if not (directory / name).is_file():
            hf_hub_download(repo_id=repo, revision=revision, filename=name,
                            local_dir=str(directory), token=token or None)


def _verify(path: Path, expected: str) -> None:
    """Verify each downloaded weight file once; remember its size and mtime."""
    stat = path.stat()
    record = {"sha256": expected, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    marker = path.with_suffix(path.suffix + ".verified.json")
    if marker.is_file():
        try:
            if json.loads(marker.read_text()) == record:
                return
        except (ValueError, OSError):
            pass
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise RuntimeError(f"Model checksum failed for {path.name}; remove that file and download it again.")
    marker.write_text(json.dumps(record), encoding="utf-8")


def speaker_core(config: Any, adapter_dim: int = 128, embedding_dim: int = 256) -> Any:
    """Construct the published layer-adapter / MFA / ASP inference architecture.

    Parameter names match the authors' trained checkpoint. All inference
    parameters must load; random or partially matched weights are rejected.
    See https://github.com/ZXHY-82/w2v-BERT-2.0_SV and docs/voice-identity-models.md.
    """
    import torch
    from torch import nn
    from transformers import Wav2Vec2BertModel

    class SpeakerCore(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.front = nn.Module()
            self.front.encoder = Wav2Vec2BertModel(config)
            if hasattr(self.front.encoder, "masked_spec_embed"):
                del self.front.encoder.masked_spec_embed
            self.adapter_layers = nn.ModuleList([
                nn.Sequential(nn.Linear(config.hidden_size, adapter_dim), nn.LayerNorm(adapter_dim),
                              nn.ReLU(), nn.Linear(adapter_dim, adapter_dim))
                for _ in range(config.num_hidden_layers + 1)
            ])
            features = adapter_dim * len(self.adapter_layers)
            self.pooling = nn.Module()
            self.pooling.attention = nn.Sequential(
                nn.Conv1d(features, adapter_dim, 1), nn.ReLU(), nn.BatchNorm1d(adapter_dim),
                nn.Conv1d(adapter_dim, features, 1), nn.Softmax(dim=2),
            )
            self.bottleneck = nn.Linear(2 * features, embedding_dim)

        def forward(self, input_features: Any) -> Any:
            # The published frontend runs projection + all Conformer layers
            # directly, without masking, padding, or training-time augmentation.
            encoder = self.front.encoder
            hidden = encoder.feature_projection(input_features)[0]
            adapted = [self.adapter_layers[0](hidden)]
            for index, layer in enumerate(encoder.encoder.layers, start=1):
                hidden = layer(hidden)[0]
                adapted.append(self.adapter_layers[index](hidden))
            frames = torch.cat(adapted, dim=-1)
            attention = self.pooling.attention(frames.transpose(1, 2)).transpose(1, 2)
            mean = (frames * attention).sum(dim=1)
            variance = ((frames.square() * attention).sum(dim=1) - mean.square()).clamp_min(1e-5)
            return self.bottleneck(torch.cat((mean, variance.sqrt()), dim=1))

    return SpeakerCore()


class SpeakerBackend:
    def __init__(self, directory: Path, device: str, token: str = "") -> None:
        import torch
        from transformers import SeamlessM4TFeatureExtractor, Wav2Vec2BertConfig

        _files(SPEAKER_MODEL, SPEAKER_REVISION, (SPEAKER_CHECKPOINT,), directory, token)
        _verify(directory / SPEAKER_CHECKPOINT, SPEAKER_SHA256)
        frontend = directory / "frontend"
        _files(FRONTEND_MODEL, FRONTEND_REVISION, ("config.json", "preprocessor_config.json"), frontend, token)
        config = Wav2Vec2BertConfig.from_pretrained(str(frontend), local_files_only=True)
        config.apply_spec_augment = False
        self.processor = SeamlessM4TFeatureExtractor.from_pretrained(str(frontend), local_files_only=True)
        checkpoint = torch.load(directory / SPEAKER_CHECKPOINT, map_location="cpu", weights_only=True, mmap=True)
        state = checkpoint.get("modules", {}).get("spk_model", checkpoint)
        if not isinstance(state, dict) or not state:
            raise RuntimeError("The trained speaker checkpoint has no speaker weights.")
        # Pruning gates in the upstream training fork are inactive in this
        # unpruned checkpoint and have no counterpart in stock Transformers.
        state = {key: value for key, value in state.items() if "hard_concrete" not in key}
        with torch.device("meta"):
            self.model = speaker_core(config)
        self.model.load_state_dict(state, strict=True, assign=True)
        self.model.eval().requires_grad_(False).to(device)
        self.device = device

    def parameters(self) -> Any:
        return self.model.parameters()

    def embed(self, waveform: Any) -> Any:
        import torch

        if hasattr(waveform, "detach"):
            waveform = waveform.detach().cpu().numpy().reshape(-1)
        features = self.processor(waveform, sampling_rate=16000, return_tensors="pt",
                                  padding=False, truncation=False, return_attention_mask=False).input_features
        with torch.inference_mode():
            embedding = self.model(features.to(self.device)).reshape(-1).float()
            if not torch.isfinite(embedding).all() or float(embedding.norm()) <= 1e-9:
                raise RuntimeError("The speaker model returned an invalid embedding.")
            return (embedding / embedding.norm()).cpu()


class EmotionBackend:
    def __init__(self, directory: Path, device: str, token: str = "") -> None:
        from funasr import AutoModel

        _files(EMOTION_MODEL, EMOTION_REVISION,
               ("config.yaml", "configuration.json", "tokens.txt", "model.pt"), directory, token)
        _verify(directory / "model.pt", EMOTION_SHA256)
        # Use installed, pinned FunASR code. Never load executable model code
        # from a model repository or download an unrelated ASR/VAD pipeline.
        self.engine = AutoModel(model=str(directory), device=device, hub="hf",
                                disable_update=True, disable_pbar=True, trust_remote_code=False)
        # FunASR can choose CPU internally when the requested accelerator is
        # unavailable. Report where its parameters actually live.
        self.device = next(self.engine.model.parameters()).device.type

    def parameters(self) -> Any:
        return self.engine.model.parameters()

    def classify(self, waveform: Any) -> dict[str, float]:
        import torch

        with torch.inference_mode():
            results = self.engine.generate(input=waveform, fs=16000, granularity="utterance",
                                           extract_embedding=False, disable_pbar=True)
        if not isinstance(results, list) or len(results) != 1:
            raise RuntimeError("Emotion2vec returned no utterance classification.")
        labels, scores = results[0].get("labels", []), results[0].get("scores", [])
        if not labels or len(labels) != len(scores):
            raise RuntimeError("Emotion2vec returned inconsistent labels and scores.")
        result = {str(label): float(score) for label, score in zip(labels, scores)}
        if not all(math.isfinite(score) and 0 <= score <= 1 for score in result.values()):
            raise RuntimeError("Emotion2vec returned invalid probabilities.")
        return result
