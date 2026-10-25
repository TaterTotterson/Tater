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
CHATTERBOX_LOCAL_TTS_BACKEND = "chatterbox_local"
DEFAULT_QWEN_TTS_MODEL = "Qwen/Qwen3-TTS-12Hz-0.6B-Base"
QWEN_TTS_VOICE_DESIGN_MODEL = "Qwen/Qwen3-TTS-12Hz-1.7B-VoiceDesign"
DEFAULT_OMNIVOICE_TTS_MODEL = "k2-fsa/OmniVoice"
DEFAULT_CHATTERBOX_LOCAL_TTS_MODEL = "ResembleAI/chatterbox-turbo"
CHATTERBOX_NANO_TTS_MODEL = "ResembleAI/chatterbox-nano"

_models: Dict[Tuple[str, str], Any] = {}
_clone_prompts: Dict[Tuple[str, str, str, int, int], Any] = {}
_chatterbox_builtin_conditionals: Dict[str, Any] = {}
_chatterbox_clone_conditionals: Dict[Tuple[str, str, int, int], Any] = {}


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
    total = 0
    found_child = False
    for child_name in ("t3", "s3gen", "ve"):
        child = getattr(model, child_name, None)
        child_parameters = getattr(child, "parameters", None)
        if not callable(child_parameters):
            continue
        found_child = True
        with contextlib.suppress(Exception):
            for parameter in child_parameters():
                numel = getattr(parameter, "numel", None)
                element_size = getattr(parameter, "element_size", None)
                if callable(numel) and callable(element_size):
                    total += int(numel()) * int(element_size())
    if found_child:
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


def _chatterbox_device() -> str:
    import torch

    requested = str(os.getenv("TATER_MANAGED_TTS_ACCELERATION") or "auto").strip().lower()
    if requested in {"cuda", "rocm"} and torch.cuda.is_available():
        return "cuda"
    mps = getattr(getattr(torch, "backends", None), "mps", None)
    if requested == "mps" and mps is not None and mps.is_available():
        return "mps"
    if requested == "auto":
        if torch.cuda.is_available():
            return "cuda"
        if mps is not None and mps.is_available():
            return "mps"
    return "cpu"


def _chatterbox_model(model_id: str) -> Any:
    selected_model = str(model_id or DEFAULT_CHATTERBOX_LOCAL_TTS_MODEL).strip()
    if selected_model not in {DEFAULT_CHATTERBOX_LOCAL_TTS_MODEL, CHATTERBOX_NANO_TTS_MODEL}:
        raise RuntimeError(f"Unsupported built-in Chatterbox model: {selected_model}")
    key = (CHATTERBOX_LOCAL_TTS_BACKEND, selected_model)
    if key in _models:
        return _models[key]

    from chatterbox.tts_turbo import ChatterboxTurboTTS

    device = _chatterbox_device()
    if device == "mps":
        os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")
    model = ChatterboxTurboTTS.from_pretrained(
        device=device,
        nano=selected_model == CHATTERBOX_NANO_TTS_MODEL,
    )
    _models[key] = model
    _chatterbox_builtin_conditionals[selected_model] = getattr(model, "conds", None)
    return model


def _optional_float(value: Any, default: float, *, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return min(float(maximum), max(float(minimum), parsed))


def _generate_chatterbox(row: Dict[str, Any]) -> tuple[Any, int]:
    import torch

    model_id = str(row.get("model") or DEFAULT_CHATTERBOX_LOCAL_TTS_MODEL).strip()
    model = _chatterbox_model(model_id)
    clone_audio = str(row.get("clone_audio") or "").strip()
    if clone_audio:
        stat = Path(clone_audio).stat()
        condition_key = (model_id, clone_audio, int(stat.st_size), int(stat.st_mtime_ns))
        conditionals = _chatterbox_clone_conditionals.get(condition_key)
        if conditionals is None:
            model.prepare_conditionals(clone_audio)
            conditionals = model.conds
            _chatterbox_clone_conditionals.clear()
            _chatterbox_clone_conditionals[condition_key] = conditionals
        model.conds = conditionals
    else:
        model.conds = _chatterbox_builtin_conditionals.get(model_id)
    if model.conds is None:
        raise RuntimeError("Built-in Chatterbox needs a reference audio file for this model.")

    seed_raw = str(row.get("seed") or "").strip()
    if seed_raw:
        try:
            torch.manual_seed(int(float(seed_raw)))
        except Exception as exc:
            raise RuntimeError("Chatterbox seed must be an integer.") from exc
    generation = {
        "text": str(row.get("text") or "").strip(),
        "temperature": _optional_float(row.get("temperature"), 0.8, minimum=0.05, maximum=2.0),
        "exaggeration": _optional_float(row.get("exaggeration"), 0.0, minimum=0.0, maximum=2.0),
        "cfg_weight": _optional_float(row.get("cfg_weight"), 0.0, minimum=0.0, maximum=1.0),
    }
    with torch.inference_mode():
        audio = model.generate(**generation)
    return audio, int(getattr(model, "sr", 0) or 24000)


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
    parser.add_argument(
        "--backend",
        choices=(QWEN_TTS_BACKEND, OMNIVOICE_TTS_BACKEND, CHATTERBOX_LOCAL_TTS_BACKEND),
        required=True,
    )
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
                    or {
                        QWEN_TTS_BACKEND: DEFAULT_QWEN_TTS_MODEL,
                        OMNIVOICE_TTS_BACKEND: DEFAULT_OMNIVOICE_TTS_MODEL,
                        CHATTERBOX_LOCAL_TTS_BACKEND: DEFAULT_CHATTERBOX_LOCAL_TTS_MODEL,
                    }[args.backend]
                ).strip()
                model_obj = (
                    _qwen_model(model_id)
                    if args.backend == QWEN_TTS_BACKEND
                    else _omnivoice_model(model_id)
                    if args.backend == OMNIVOICE_TTS_BACKEND
                    else _chatterbox_model(model_id)
                )
                _respond({"id": request_id, "ok": True, **_loaded_model_info(args.backend, model_id, model_obj)})
                continue
            if action != "synthesize":
                raise ValueError(f"Unsupported worker action: {action}")
            if args.backend == QWEN_TTS_BACKEND:
                audio, sample_rate = _generate_qwen(row)
            elif args.backend == OMNIVOICE_TTS_BACKEND:
                audio, sample_rate = _generate_omnivoice(row)
            else:
                audio, sample_rate = _generate_chatterbox(row)
            model_id = str(
                row.get("model")
                or {
                    QWEN_TTS_BACKEND: DEFAULT_QWEN_TTS_MODEL,
                    OMNIVOICE_TTS_BACKEND: DEFAULT_OMNIVOICE_TTS_MODEL,
                    CHATTERBOX_LOCAL_TTS_BACKEND: DEFAULT_CHATTERBOX_LOCAL_TTS_MODEL,
                }[args.backend]
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
