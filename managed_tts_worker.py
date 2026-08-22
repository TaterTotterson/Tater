#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Tuple


QWEN_TTS_BACKEND = "qwen3_tts"
OMNIVOICE_TTS_BACKEND = "omnivoice"
DEFAULT_QWEN_TTS_MODEL = "Qwen/Qwen3-TTS-12Hz-0.6B-Base"
QWEN_TTS_VOICE_DESIGN_MODEL = "Qwen/Qwen3-TTS-12Hz-1.7B-VoiceDesign"
DEFAULT_OMNIVOICE_TTS_MODEL = "k2-fsa/OmniVoice"

_models: Dict[Tuple[str, str], Any] = {}
_clone_prompts: Dict[Tuple[str, str, str, int, int], Any] = {}


def _model_estimated_bytes(model: Any) -> int:
    get_memory_footprint = getattr(model, "get_memory_footprint", None)
    if callable(get_memory_footprint):
        with contextlib.suppress(Exception):
            return max(0, int(get_memory_footprint()))
    parameters = getattr(model, "parameters", None)
    if callable(parameters):
        total = 0
        with contextlib.suppress(Exception):
            for parameter in parameters():
                numel = getattr(parameter, "numel", None)
                element_size = getattr(parameter, "element_size", None)
                if callable(numel) and callable(element_size):
                    total += int(numel()) * int(element_size())
            return max(0, total)
    return 0


def _model_device(model: Any, backend: str) -> str:
    if backend == QWEN_TTS_BACKEND and _apple_silicon():
        return "MLX / Metal"
    for value in (
        getattr(model, "device", None),
        getattr(model, "device_map", None),
    ):
        token = str(value or "").strip()
        if token:
            return token
    parameters = getattr(model, "parameters", None)
    if callable(parameters):
        with contextlib.suppress(Exception):
            first = next(iter(parameters()))
            token = str(getattr(first, "device", "") or "").strip()
            if token:
                return token
    return "CPU"


def _loaded_model_info(backend: str, model_id: str, model: Any) -> Dict[str, Any]:
    return {
        "loaded": True,
        "backend": backend,
        "model": model_id,
        "device": _model_device(model, backend),
        "estimated_bytes": _model_estimated_bytes(model),
        "loaded_ts": time.time(),
    }


def _apple_silicon() -> bool:
    return sys.platform == "darwin" and platform.machine().lower() in {"arm64", "aarch64"}


def _qwen_model(model_id: str) -> Any:
    key = (QWEN_TTS_BACKEND, model_id)
    if key in _models:
        return _models[key]
    if _apple_silicon():
        from mlx_audio.tts import load

        mlx_model_id = {
            DEFAULT_QWEN_TTS_MODEL: "mlx-community/Qwen3-TTS-12Hz-0.6B-Base-6bit",
            QWEN_TTS_VOICE_DESIGN_MODEL: "mlx-community/Qwen3-TTS-12Hz-1.7B-VoiceDesign-6bit",
        }.get(model_id, model_id)
        model = load(mlx_model_id)
    else:
        import torch
        from qwen_tts import Qwen3TTSModel

        if torch.cuda.is_available():
            dtype = torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16
            device = "cuda:0"
        else:
            dtype = torch.float32
            device = "cpu"
        model = Qwen3TTSModel.from_pretrained(
            model_id,
            device_map=device,
            dtype=dtype,
            attn_implementation="sdpa",
        )
    _models[key] = model
    return model


def _result_audio(result: Any) -> Any:
    if hasattr(result, "__next__"):
        try:
            return next(result)
        except StopIteration as exc:
            raise RuntimeError("TTS model returned an empty result.") from exc
    if isinstance(result, (list, tuple)):
        if not result:
            raise RuntimeError("TTS model returned an empty result.")
        return result[0]
    return result


def _generate_qwen(row: Dict[str, Any]) -> tuple[Any, int]:
    model_id = str(row.get("model") or DEFAULT_QWEN_TTS_MODEL).strip()
    model = _qwen_model(model_id)
    text = str(row.get("text") or "").strip()
    language = str(row.get("language") or "English").strip() or "English"
    clone_audio = str(row.get("clone_audio") or "").strip()
    clone_text = str(row.get("clone_text") or "").strip()
    instruct = str(row.get("instruct") or "").strip()

    if _apple_silicon():
        if clone_audio and model_id != QWEN_TTS_VOICE_DESIGN_MODEL:
            if not clone_text:
                raise RuntimeError(
                    "Qwen voice cloning on Apple Silicon could not detect the reference transcript. "
                    "Enter it under Reference transcript in Settings > Speech and try again."
                )
            results = model.batch_generate(
                texts=[text],
                ref_audio=clone_audio,
                ref_text=clone_text,
                lang_code=language,
                max_tokens=1024,
            )
        else:
            results = model.batch_generate(
                texts=[text],
                instructs=[instruct or "A clear, natural, friendly voice."],
                lang_code=language,
                max_tokens=1024,
            )
        result = _result_audio(results)
        audio = getattr(result, "audio", result)
        sample_rate = int(getattr(result, "sample_rate", 0) or getattr(model, "sample_rate", 0) or 24000)
        return audio, sample_rate

    if clone_audio and model_id != QWEN_TTS_VOICE_DESIGN_MODEL:
        stat = Path(clone_audio).stat()
        prompt_key = (model_id, clone_audio, clone_text, int(stat.st_size), int(stat.st_mtime_ns))
        clone_prompt = _clone_prompts.get(prompt_key)
        if clone_prompt is None:
            clone_prompt = model.create_voice_clone_prompt(
                ref_audio=clone_audio,
                ref_text=clone_text or None,
                x_vector_only_mode=not bool(clone_text),
            )
            _clone_prompts.clear()
            _clone_prompts[prompt_key] = clone_prompt
        wavs, sample_rate = model.generate_voice_clone(
            text=[text],
            language=[language],
            voice_clone_prompt=clone_prompt,
        )
    else:
        wavs, sample_rate = model.generate_voice_design(
            text=[text],
            language=[language],
            instruct=[instruct or "A clear, natural, friendly voice."],
        )
    return _result_audio(wavs), int(sample_rate)


def _omnivoice_model(model_id: str) -> Any:
    key = (OMNIVOICE_TTS_BACKEND, model_id)
    if key in _models:
        return _models[key]
    import torch
    from omnivoice import OmniVoice

    if torch.cuda.is_available():
        dtype = torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16
        device_map = "cuda:0"
    elif getattr(getattr(torch, "backends", None), "mps", None) is not None and torch.backends.mps.is_available():
        dtype = torch.float32
        device_map = "mps"
    else:
        dtype = torch.float32
        device_map = "cpu"
    kwargs: Dict[str, Any] = {"device_map": device_map, "dtype": dtype}
    try:
        model = OmniVoice.from_pretrained(model_id, attn_implementation="sdpa" if device_map.startswith("cuda") else "eager", **kwargs)
    except TypeError:
        model = OmniVoice.from_pretrained(model_id, **kwargs)
    _models[key] = model
    return model


def _generate_omnivoice(row: Dict[str, Any]) -> tuple[Any, int]:
    model_id = str(row.get("model") or DEFAULT_OMNIVOICE_TTS_MODEL).strip()
    model = _omnivoice_model(model_id)
    clone_audio = str(row.get("clone_audio") or "").strip()
    clone_text = str(row.get("clone_text") or "").strip()
    generation: Dict[str, Any] = {
        "text": str(row.get("text") or "").strip(),
        "language": str(row.get("language") or "").strip() or None,
    }
    if clone_audio:
        stat = Path(clone_audio).stat()
        prompt_key = (model_id, clone_audio, clone_text, int(stat.st_size), int(stat.st_mtime_ns))
        clone_prompt = _clone_prompts.get(prompt_key)
        create_prompt = getattr(model, "create_voice_clone_prompt", None)
        if clone_prompt is None and callable(create_prompt):
            clone_prompt = create_prompt(ref_audio=clone_audio, ref_text=clone_text or None)
            _clone_prompts.clear()
            _clone_prompts[prompt_key] = clone_prompt
        if clone_prompt is not None:
            generation["voice_clone_prompt"] = clone_prompt
        else:
            generation["ref_audio"] = clone_audio
            generation["ref_text"] = clone_text or None
    else:
        instruct = str(row.get("instruct") or "").strip()
        if instruct:
            generation["instruct"] = instruct
    wavs = model.generate(**generation)
    return _result_audio(wavs), int(getattr(model, "sampling_rate", 0) or 24000)


def _write_wav(path: str, audio: Any, sample_rate: int) -> None:
    import numpy as np
    import soundfile as sf

    if hasattr(audio, "detach"):
        audio = audio.detach().float().cpu().numpy()
    array = np.asarray(audio, dtype=np.float32).squeeze()
    if array.ndim != 1:
        array = array.reshape(-1)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    sf.write(str(target), array, int(sample_rate), subtype="PCM_16", format="WAV")


def _respond(row: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(row, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=(QWEN_TTS_BACKEND, OMNIVOICE_TTS_BACKEND), required=True)
    args = parser.parse_args()
    for raw in sys.stdin:
        request_id = ""
        try:
            row = json.loads(raw)
            request_id = str(row.get("id") or "")
            action = str(row.get("action") or "")
            if action == "shutdown":
                return 0
            if action == "ping":
                loaded_info: Dict[str, Any] = {}
                if _models:
                    (loaded_backend, loaded_model), model_obj = next(iter(_models.items()))
                    loaded_info = _loaded_model_info(loaded_backend, loaded_model, model_obj)
                _respond({"id": request_id, "ok": True, **loaded_info})
                continue
            if action == "load":
                model_id = str(
                    row.get("model")
                    or (DEFAULT_QWEN_TTS_MODEL if args.backend == QWEN_TTS_BACKEND else DEFAULT_OMNIVOICE_TTS_MODEL)
                ).strip()
                model_obj = _qwen_model(model_id) if args.backend == QWEN_TTS_BACKEND else _omnivoice_model(model_id)
                _respond({"id": request_id, "ok": True, **_loaded_model_info(args.backend, model_id, model_obj)})
                continue
            if action != "synthesize":
                raise ValueError(f"Unsupported worker action: {action}")
            if args.backend == QWEN_TTS_BACKEND:
                audio, sample_rate = _generate_qwen(row)
            else:
                audio, sample_rate = _generate_omnivoice(row)
            model_id = str(
                row.get("model")
                or (DEFAULT_QWEN_TTS_MODEL if args.backend == QWEN_TTS_BACKEND else DEFAULT_OMNIVOICE_TTS_MODEL)
            ).strip()
            model_obj = _models.get((args.backend, model_id))
            output_path = str(row.get("output_path") or "").strip()
            if not output_path:
                raise ValueError("Worker output path is required.")
            _write_wav(output_path, audio, sample_rate)
            _respond(
                {
                    "id": request_id,
                    "ok": True,
                    "output_path": output_path,
                    "sample_rate": int(sample_rate),
                    **(_loaded_model_info(args.backend, model_id, model_obj) if model_obj is not None else {}),
                }
            )
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            _respond({"id": request_id, "ok": False, "error": str(exc) or exc.__class__.__name__})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
