from __future__ import annotations

import contextlib
from typing import Any, Dict, List

from helpers import redis_client
from . import ui_helpers as esphome_ui_helpers


VOICE_MODEL_SETTING_GROUPS = [
    (
        "SpeechBrain Models",
        ["VOICE_SPEECHBRAIN_ACCELERATION"],
    ),
    (
        "openWakeWord",
        [
            "VOICE_OPENWAKEWORD_ENABLED",
            "VOICE_OPENWAKEWORD_MODEL_SOURCE",
            "VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK",
            "VOICE_OPENWAKEWORD_DEVICE",
            "VOICE_OPENWAKEWORD_THRESHOLD",
            "VOICE_OPENWAKEWORD_PATIENCE",
            "VOICE_OPENWAKEWORD_DEBOUNCE_S",
            "VOICE_OPENWAKEWORD_VAD_THRESHOLD",
            "VOICE_OPENWAKEWORD_STREAM_QUEUE_MAX",
            "VOICE_OPENWAKEWORD_DROP_QUEUED_FRAMES",
            "VOICE_OPENWAKEWORD_DIAGNOSTIC_LOGGING",
        ],
    ),
    (
        "NanoWakeWord",
        [
            "VOICE_NANOWAKEWORD_ENABLED",
            "VOICE_NANOWAKEWORD_MODEL_SOURCE",
            "VOICE_NANOWAKEWORD_DEVICE",
            "VOICE_NANOWAKEWORD_THRESHOLD",
            "VOICE_NANOWAKEWORD_PATIENCE",
            "VOICE_NANOWAKEWORD_DEBOUNCE_S",
            "VOICE_NANOWAKEWORD_STREAM_QUEUE_MAX",
            "VOICE_NANOWAKEWORD_DROP_QUEUED_FRAMES",
            "VOICE_NANOWAKEWORD_DIAGNOSTIC_LOGGING",
        ],
    ),
]

VOICE_MODEL_SETTING_KEYS = {
    key
    for _label, keys in VOICE_MODEL_SETTING_GROUPS
    for key in keys
}

LEGACY_RUNTIME_SETTING_KEYS = {
    "VOICE_WAKE_STARTUP_GATE_S",
    "VOICE_WAKE_SILENCE_SECONDS",
    "VOICE_WAKE_NO_SPEECH_TIMEOUT_S",
    "VOICE_WAKE_MIN_SPEECH_FRAMES",
    "VOICE_WAKE_MIN_SILENCE_SHORT_S",
    "VOICE_WAKE_MIN_SILENCE_LONG_S",
    "VOICE_WAKE_PREROLL_S",
}


def _vp():
    from . import voice_pipeline as vp

    return vp


def settings_hash_key() -> str:
    vp = _vp()
    return str(vp.VOICE_CORE_SETTINGS_HASH_KEY or "voice_core_settings")


def voice_ui_setting_specs() -> List[Dict[str, Any]]:
    vp = _vp()
    return [
        {
            "key": "VOICE_NATIVE_DEBUG",
            "label": "Native Voice Debug Logs",
            "type": "checkbox",
            "default": False,
            "description": "Enable verbose voice pipeline logs.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_ENABLED",
            "label": "Continued Chat (Auto Reopen Mic)",
            "type": "checkbox",
            "default": vp.DEFAULT_CONTINUED_CHAT_ENABLED,
            "description": "If enabled, Tater uses a small AI check to decide whether to reopen the mic for a follow-up reply.",
        },
        {
            "key": "VOICE_STARTUP_GATE_S",
            "label": "Startup Audio Gate (sec)",
            "type": "number",
            "default": vp.DEFAULT_STARTUP_GATE_S,
            "min": 0.0,
            "max": 2.0,
            "step": 0.05,
            "description": "Ignored audio window at the start of a listening turn. Leave at 0 unless a device sends a click/pop at session start.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_STARTUP_GATE_S",
            "label": "Reopen Startup Gate (sec)",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_STARTUP_GATE_S,
            "min": 0.0,
            "max": 2.0,
            "step": 0.05,
            "description": "Ignored audio window after continued-chat mic reopen. First wake listens use a near-zero gate so early words are not clipped.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_PREROLL_S",
            "label": "Wake/Reopen Pre-Roll (sec)",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_PREROLL_S,
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
            "description": "Buffered audio kept from the wake/reopen gate so fast speech can still reach STT without driving endpointing.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_SILENCE_SECONDS",
            "label": "Wake/Reopen Silence Seconds",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_SILENCE_SECONDS,
            "min": 0.2,
            "max": 5.0,
            "step": 0.05,
            "description": "Minimum silence used for wake-word and continued-chat reopened listening.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_TIMEOUT_SECONDS",
            "label": "Wake/Reopen Max Listen Seconds",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_TIMEOUT_SECONDS,
            "min": 2.0,
            "max": 60.0,
            "step": 0.1,
            "description": "Hard cap for wake-word and continued-chat reopened listening.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_NO_SPEECH_TIMEOUT_S",
            "label": "Wake/Reopen No-Speech Seconds",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_NO_SPEECH_TIMEOUT_S,
            "min": 1.0,
            "max": 20.0,
            "step": 0.1,
            "description": "No-speech timeout used for wake-word and continued-chat reopened listening.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_FRAMES",
            "label": "Wake/Reopen Min Silence Frames",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_FRAMES,
            "min": 3,
            "max": 60,
            "step": 1,
            "description": "Minimum silence frames used for wake-word and continued-chat reopened listening.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_SHORT_S",
            "label": "Wake/Reopen Short Silence",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_SHORT_S,
            "min": 0.1,
            "max": 5.0,
            "step": 0.05,
            "description": "Minimum silence before ending short wake-word or continued-chat replies.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_LONG_S",
            "label": "Wake/Reopen Long Silence",
            "type": "number",
            "default": vp.DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_LONG_S,
            "min": 0.1,
            "max": 5.0,
            "step": 0.05,
            "description": "Minimum silence before ending longer wake-word or continued-chat replies.",
        },
        {
            "key": "VOICE_AUDIO_STALL_TIMEOUT_S",
            "label": "Audio Stall After Speech (sec)",
            "type": "number",
            "default": vp.DEFAULT_AUDIO_STALL_TIMEOUT_S,
            "min": 0.4,
            "max": 20.0,
            "step": 0.05,
            "description": "Fallback finalization delay when audio stops after speech. Tater also keeps this above the active VAD silence setting.",
        },
        {
            "key": "VOICE_AUDIO_STALL_NO_SPEECH_TIMEOUT_S",
            "label": "Audio Stall No-Speech (sec)",
            "type": "number",
            "default": vp.DEFAULT_AUDIO_STALL_NO_SPEECH_TIMEOUT_S,
            "min": 1.0,
            "max": 30.0,
            "step": 0.1,
            "description": "Fallback finalization delay when audio is flowing or stops but server VAD has not detected speech.",
        },
        {
            "key": "VOICE_BLANK_WAKE_TIMEOUT_S",
            "label": "Blank Listen Timeout (sec)",
            "type": "number",
            "default": vp.DEFAULT_BLANK_WAKE_TIMEOUT_S,
            "min": 1.0,
            "max": 20.0,
            "step": 0.1,
            "description": "Timeout for listen sessions that were not started by a wake word or continued-chat reopen.",
        },
        {
            "key": "VOICE_WAKE_ARBITRATION_ENABLED",
            "label": "Wake Arbitration",
            "type": "checkbox",
            "default": vp.DEFAULT_WAKE_ARBITRATION_ENABLED,
            "description": "If enabled, Tater resolves near-simultaneous wake triggers so only the best matching satellite handles the turn.",
        },
        {
            "key": "VOICE_WAKE_ARBITRATION_WINDOW_MS",
            "label": "Wake Arbitration Window (ms)",
            "type": "number",
            "default": vp.DEFAULT_WAKE_ARBITRATION_WINDOW_MS,
            "min": 100,
            "max": 3000,
            "step": 50,
            "description": "How long Tater waits for nearby wake candidates before stopping duplicate satellites.",
        },
        {
            "key": "VOICE_WAKE_ARBITRATION_BUSY_TIMEOUT_S",
            "label": "Wake Room Busy Timeout (sec)",
            "type": "number",
            "default": vp.DEFAULT_WAKE_ARBITRATION_BUSY_TIMEOUT_S,
            "min": 5.0,
            "max": 300.0,
            "step": 1.0,
            "description": "Safety hold while one satellite owns a room. Normal turns release earlier when playback ends; follow-up turns hold through the reopen window.",
        },
        {
            "key": "VOICE_WAKE_ARBITRATION_SCOPE",
            "label": "Wake Arbitration Scope",
            "type": "select",
            "default": vp.DEFAULT_WAKE_ARBITRATION_SCOPE,
            "options": [
                {"value": "same_area", "label": "Same room / unknown room"},
                {"value": "global", "label": "Whole home"},
            ],
            "description": "Same room is safer when multiple people may use Tater in different rooms at once. Whole home is useful when nearby satellites are assigned to different rooms.",
        },
        {
            "key": "VOICE_INPUT_GAIN",
            "label": "Input Gain",
            "type": "number",
            "default": vp.DEFAULT_AUDIO_INPUT_GAIN,
            "min": 0.5,
            "max": 16.0,
            "step": 0.1,
            "description": "Server-side gain applied to satellite microphone audio before VAD and STT. Raise this if normal speech logs low audio_peak_dbfs values.",
        },
        {
            "key": "VOICE_FASTER_WHISPER_BEAM_SIZE",
            "label": "Faster Whisper Beam Size",
            "type": "number",
            "default": vp.DEFAULT_FASTER_WHISPER_BEAM_SIZE,
            "min": 1,
            "max": 8,
            "step": 1,
            "description": "Final Faster Whisper decode quality. 1 is fastest and least accurate; 5 is a good quality default on CUDA hosts. Partial STT always stays at 1.",
        },
        {
            "key": "VOICE_FASTER_WHISPER_COMPUTE_TYPE",
            "label": "Faster Whisper Compute Type",
            "type": "select",
            "default": vp.DEFAULT_FASTER_WHISPER_COMPUTE_TYPE_SETTING,
            "options": [
                {"value": "auto", "label": "Auto"},
                {"value": "int8", "label": "Int8"},
                {"value": "float32", "label": "Float32"},
                {"value": "float16", "label": "Float16"},
                {"value": "int8_float32", "label": "Int8 + Float32"},
                {"value": "int8_float16", "label": "Int8 + Float16"},
            ],
            "description": "Auto uses float16 on newer CUDA GPUs and int8 on older CUDA cards such as Pascal/GTX 10-series. Override this if Faster Whisper reports an unsupported compute type.",
        },
        {
            "key": "VOICE_FASTER_WHISPER_INITIAL_PROMPT",
            "label": "Faster Whisper Prompt",
            "type": "textarea",
            "default": vp.DEFAULT_FASTER_WHISPER_INITIAL_PROMPT,
            "description": "Optional vocabulary hint for Faster Whisper. Use off to disable the built-in Tater/home-assistant prompt.",
        },
        {
            "key": "VOICE_VAD_BACKEND",
            "label": "VAD Backend",
            "type": "select",
            "default": vp.DEFAULT_VAD_BACKEND,
            "options": [
                {"value": "silero", "label": "Silero"},
                {"value": "webrtc", "label": "WebRTC (lightweight)"},
                {"value": "auto", "label": "Auto"},
            ],
            "description": "Silero is the best default. WebRTC is much lighter for low-power PCs and Pi-class hosts.",
        },
        {
            "key": "VOICE_VAD_SILENCE_SECONDS",
            "label": "VAD Silence Seconds",
            "type": "number",
            "default": vp.DEFAULT_VAD_SILENCE_SECONDS,
            "min": 0.2,
            "max": 5.0,
            "step": 0.05,
            "description": "How much silence ends a normal spoken turn after speech has started.",
        },
        {
            "key": "VOICE_VAD_TIMEOUT_SECONDS",
            "label": "VAD Max Listen Seconds",
            "type": "number",
            "default": vp.DEFAULT_VAD_TIMEOUT_SECONDS,
            "min": 2.0,
            "max": 60.0,
            "step": 0.1,
            "description": "Hard cap for one listening turn.",
        },
        {
            "key": "VOICE_VAD_NO_SPEECH_TIMEOUT_S",
            "label": "VAD No-Speech Seconds",
            "type": "number",
            "default": vp.DEFAULT_VAD_NO_SPEECH_TIMEOUT_S,
            "min": 1.0,
            "max": 20.0,
            "step": 0.1,
            "description": "How long to wait for speech before finalizing and letting STT recover any buffered audio.",
        },
        {
            "key": "VOICE_SILERO_THRESHOLD",
            "label": "Silero Speech Threshold",
            "type": "number",
            "default": vp.DEFAULT_SILERO_THRESHOLD,
            "min": 0.01,
            "max": 0.99,
            "step": 0.01,
            "description": "Higher values require stronger speech probability before a turn starts.",
        },
        {
            "key": "VOICE_SILERO_NEG_THRESHOLD",
            "label": "Silero Silence Threshold",
            "type": "number",
            "default": vp.DEFAULT_SILERO_NEG_THRESHOLD,
            "min": 0.0,
            "max": 0.99,
            "step": 0.01,
            "description": "Below this value, audio counts as silence once speech has started.",
        },
        {
            "key": "VOICE_SILERO_MIN_SPEECH_FRAMES",
            "label": "Min Speech Frames",
            "type": "number",
            "default": vp.DEFAULT_SILERO_MIN_SPEECH_FRAMES,
            "min": 1,
            "max": 30,
            "step": 1,
            "description": "Consecutive speech frames required before Tater considers the user to be speaking.",
        },
        {
            "key": "VOICE_SILERO_MIN_SILENCE_FRAMES",
            "label": "Min Silence Frames",
            "type": "number",
            "default": vp.DEFAULT_SILERO_MIN_SILENCE_FRAMES,
            "min": 3,
            "max": 60,
            "step": 1,
            "description": "Consecutive silence frames required before frame-count endpointing can end a turn.",
        },
        {
            "key": "VOICE_VAD_MIN_SILENCE_SHORT_S",
            "label": "Short Command Silence",
            "type": "number",
            "default": vp.DEFAULT_VAD_MIN_SILENCE_SHORT_S,
            "min": 0.1,
            "max": 5.0,
            "step": 0.05,
            "description": "Minimum elapsed silence before ending short commands.",
        },
        {
            "key": "VOICE_VAD_MIN_SILENCE_LONG_S",
            "label": "Long Command Silence",
            "type": "number",
            "default": vp.DEFAULT_VAD_MIN_SILENCE_LONG_S,
            "min": 0.1,
            "max": 5.0,
            "step": 0.05,
            "description": "Minimum elapsed silence before ending longer commands.",
        },
        {
            "key": "VOICE_WEBRTC_VAD_AGGRESSIVENESS",
            "label": "WebRTC VAD Aggressiveness",
            "type": "number",
            "default": vp.DEFAULT_WEBRTC_VAD_AGGRESSIVENESS,
            "min": 0,
            "max": 3,
            "step": 1,
            "description": "WebRTC only. 0 is least aggressive; 3 filters non-speech most aggressively.",
        },
        {
            "key": "VOICE_SPEECHBRAIN_ACCELERATION",
            "label": "SpeechBrain Acceleration",
            "type": "select",
            "default": vp.DEFAULT_SPEECHBRAIN_ACCELERATION,
            "options": [
                {"value": "auto", "label": "Auto"},
                {"value": "cpu", "label": "CPU"},
                {"value": "cuda", "label": "NVIDIA CUDA"},
            ],
            "description": "Controls SpeechBrain models used by Speaker ID and Emotion ID. Auto uses CUDA when available and falls back to CPU if CUDA load fails.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_ENABLED",
            "label": "Enable Tater openWakeWord",
            "type": "checkbox",
            "default": True,
            "description": "Enables Tater's openWakeWord detector for satellites that stream wake audio to /api/openwakeword/stream. Turn off only when every satellite is using device-local microWakeWord.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_MODEL_SOURCE",
            "label": "openWakeWord Model",
            "type": "select",
            "default": "hey_jarvis",
            "options": [],
            "description": "Choose a prebuilt openWakeWord model or one downloaded from the trainer.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK",
            "label": "openWakeWord Runtime",
            "type": "select",
            "default": "onnx",
            "options": [
                {"value": "onnx", "label": "ONNX"},
                {"value": "tflite", "label": "TFLite"},
            ],
            "description": "ONNX is preferred for Tater hosts and can use the NVIDIA image's accelerated runtime where supported.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_DEVICE",
            "label": "openWakeWord Device",
            "type": "select",
            "default": "auto",
            "options": [
                {"value": "auto", "label": "Auto"},
                {"value": "cpu", "label": "CPU"},
                {"value": "gpu", "label": "GPU / CUDA"},
            ],
            "description": "Auto uses CUDA-capable ONNX Runtime when available and otherwise falls back to CPU.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_THRESHOLD",
            "label": "openWakeWord Threshold",
            "type": "number",
            "default": 0.5,
            "min": 0.01,
            "max": 0.99,
            "step": 0.01,
            "description": "Higher values are stricter and reduce false wake triggers. 0.5 matches the old Home Assistant openWakeWord default.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_PATIENCE",
            "label": "openWakeWord Patience",
            "type": "number",
            "default": 1,
            "min": 1,
            "max": 10,
            "step": 1,
            "description": "How many consecutive model hits are required before Tater reports a remote wake. 1 is closest to the old Home Assistant openWakeWord behavior.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_DEBOUNCE_S",
            "label": "openWakeWord Debounce (sec)",
            "type": "number",
            "default": 2.0,
            "min": 0.0,
            "max": 30.0,
            "step": 0.1,
            "description": "Minimum time between accepted remote openWakeWord detections from one satellite.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_VAD_THRESHOLD",
            "label": "openWakeWord VAD Threshold",
            "type": "number",
            "default": 0.0,
            "min": 0.0,
            "max": 0.99,
            "step": 0.01,
            "description": "Optional openWakeWord internal VAD gate. Leave at 0 unless you are tuning false wakes.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_STREAM_QUEUE_MAX",
            "label": "openWakeWord Stream Queue",
            "type": "number",
            "default": 12,
            "min": 1,
            "max": 120,
            "step": 1,
            "description": "How many remote wake audio chunks can wait while Tater is busy. Higher values reduce drops but can add detection latency.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_DROP_QUEUED_FRAMES",
            "label": "Drop Queued openWakeWord Frames",
            "type": "checkbox",
            "default": True,
            "description": "When enabled, old queued wake audio is discarded if Tater falls behind. Turn off to preserve every frame and let the stream backpressure instead.",
        },
        {
            "key": "VOICE_OPENWAKEWORD_DIAGNOSTIC_LOGGING",
            "label": "openWakeWord Diagnostic Logs",
            "type": "checkbox",
            "default": True,
            "description": "Log best-label, score, threshold, and hit-count details for remote openWakeWord tuning. Turn off once the model is tuned if you want quieter logs.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_ENABLED",
            "label": "Enable Tater NanoWakeWord",
            "type": "checkbox",
            "default": True,
            "description": "Enables Tater's NanoWakeWord detector for satellites that stream wake audio to /api/nanowakeword/stream.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_MODEL_SOURCE",
            "label": "NanoWakeWord Model",
            "type": "select",
            "default": "",
            "options": [],
            "description": "Choose a downloaded/local NanoWakeWord model or one pulled from a NanoWakeWord trainer.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_DEVICE",
            "label": "NanoWakeWord Device",
            "type": "select",
            "default": "auto",
            "options": [
                {"value": "auto", "label": "Auto"},
                {"value": "cpu", "label": "CPU"},
                {"value": "gpu", "label": "GPU / CUDA"},
            ],
            "description": "Auto uses CUDA for NanoWakeWord when the selected runtime supports it and otherwise falls back to CPU.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_THRESHOLD",
            "label": "NanoWakeWord Threshold",
            "type": "number",
            "default": 0.90,
            "min": 0.01,
            "max": 0.99,
            "step": 0.01,
            "description": "Higher values are stricter and reduce false wake triggers.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_PATIENCE",
            "label": "NanoWakeWord Patience",
            "type": "number",
            "default": 2,
            "min": 1,
            "max": 10,
            "step": 1,
            "description": "How many consecutive model hits are required before Tater reports a remote wake.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_DEBOUNCE_S",
            "label": "NanoWakeWord Debounce (sec)",
            "type": "number",
            "default": 4.0,
            "min": 0.0,
            "max": 30.0,
            "step": 0.1,
            "description": "Minimum time between accepted remote NanoWakeWord detections from one satellite.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_STREAM_QUEUE_MAX",
            "label": "NanoWakeWord Stream Queue",
            "type": "number",
            "default": 12,
            "min": 1,
            "max": 120,
            "step": 1,
            "description": "How many remote wake audio chunks can wait while Tater is busy. Higher values reduce drops but can add detection latency.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_DROP_QUEUED_FRAMES",
            "label": "Drop Queued NanoWakeWord Frames",
            "type": "checkbox",
            "default": True,
            "description": "When enabled, old queued wake audio is discarded if Tater falls behind. Turn off to preserve every frame and let the stream backpressure instead.",
        },
        {
            "key": "VOICE_NANOWAKEWORD_DIAGNOSTIC_LOGGING",
            "label": "NanoWakeWord Diagnostic Logs",
            "type": "checkbox",
            "default": False,
            "description": "Log score, threshold, and hit-count details for remote NanoWakeWord tuning. Leave off during normal use.",
        },
        {
            "key": "VOICE_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED",
            "label": "Live Tool Progress Speech",
            "type": "checkbox",
            "default": vp.DEFAULT_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED,
            "description": "If enabled, Tater can briefly speak Hydra tool-progress updates before the final reply. Updated Tater firmware on VoicePE or Satellite1 can also show the tool-call LED animation during those spoken updates. If disabled, Tater stays in thinking until the final response.",
        },
        {
            "key": "VOICE_EXPERIMENTAL_PARTIAL_STT_ENABLED",
            "label": "Partial STT",
            "type": "checkbox",
            "default": vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_ENABLED,
            "description": "If enabled, Tater will try to build live partial transcripts during capture for local and Wyoming STT backends. This can improve turn-end decisions, but may use more CPU/GPU.",
        },
        {
            "key": "VOICE_EXPERIMENTAL_TTS_EARLY_START_ENABLED",
            "label": "Early-Start TTS",
            "type": "checkbox",
            "default": vp.DEFAULT_EXPERIMENTAL_TTS_EARLY_START_ENABLED,
            "description": "If enabled, Tater may start speaking long replies sooner by splitting playback into early chunks. This may introduce slightly more audible sentence gaps.",
        },
        {
            "key": "VOICE_DISCOVERY_ENABLED",
            "label": "Enable mDNS Discovery",
            "type": "checkbox",
            "default": True,
            "description": "Discover ESPHome satellites via mDNS.",
        },
        {
            "key": "VOICE_DISCOVERY_SCAN_SECONDS",
            "label": "Discovery Scan Interval (sec)",
            "type": "number",
            "default": vp.DEFAULT_DISCOVERY_SCAN_SECONDS,
            "min": 5,
            "max": 600,
        },
        {
            "key": "VOICE_DISCOVERY_MDNS_TIMEOUT_S",
            "label": "mDNS Listen Window (sec)",
            "type": "number",
            "default": vp.DEFAULT_DISCOVERY_MDNS_TIMEOUT_S,
            "min": 0.5,
            "max": 20.0,
            "step": 0.1,
        },
        {
            "key": "VOICE_ESPHOME_API_PORT",
            "label": "ESPHome API Port",
            "type": "number",
            "default": vp.DEFAULT_ESPHOME_API_PORT,
            "min": 1,
            "max": 65535,
        },
        {
            "key": "VOICE_ESPHOME_PASSWORD",
            "label": "ESPHome API Password",
            "type": "password",
            "default": "",
        },
        {
            "key": "VOICE_ESPHOME_NOISE_PSK",
            "label": "ESPHome Noise PSK",
            "type": "password",
            "default": "",
        },
        {
            "key": "VOICE_ESPHOME_CONNECT_TIMEOUT_S",
            "label": "ESPHome Connect Timeout (sec)",
            "type": "number",
            "default": vp.DEFAULT_ESPHOME_CONNECT_TIMEOUT_S,
            "min": 2.0,
            "max": 60.0,
            "step": 0.1,
        },
        {
            "key": "VOICE_ESPHOME_RETRY_SECONDS",
            "label": "ESPHome Retry Seconds",
            "type": "number",
            "default": vp.DEFAULT_ESPHOME_RETRY_SECONDS,
            "min": 2,
            "max": 300,
        },
        {
            "key": "VOICE_NATIVE_WYOMING_TIMEOUT_S",
            "label": "Wyoming Timeout (sec)",
            "type": "number",
            "default": vp.DEFAULT_WYOMING_TIMEOUT_SECONDS,
            "min": 5.0,
            "max": 300.0,
            "step": 0.5,
        },
        {
            "key": "VOICE_WYOMING_STT_QUEUE_CHUNKS",
            "label": "Wyoming STT Queue Chunks",
            "type": "number",
            "default": vp.DEFAULT_WYOMING_STT_QUEUE_CHUNKS,
            "min": 8,
            "max": 512,
            "step": 1,
            "description": "Maximum queued microphone chunks while streaming to Wyoming STT before Tater starts dropping chunks and falls back to buffered local STT for that turn.",
        },
        {
            "key": "VOICE_WYOMING_STT_QUEUE_DROP_LIMIT",
            "label": "Wyoming STT Drop Limit",
            "type": "number",
            "default": vp.DEFAULT_WYOMING_STT_QUEUE_DROP_LIMIT,
            "min": 1,
            "max": 64,
            "step": 1,
            "description": "How many Wyoming STT stream chunks may be dropped before the turn is marked unhealthy and retried with buffered local STT.",
        },
    ]


def _voice_ui_spec_map() -> Dict[str, Dict[str, Any]]:
    vp = _vp()
    out: Dict[str, Dict[str, Any]] = {}
    for spec in voice_ui_setting_specs():
        if not isinstance(spec, dict):
            continue
        key = vp._text(spec.get("key"))
        if not key:
            continue
        out[key] = dict(spec)
    return out


def _voice_ui_field_value(spec: Dict[str, Any], raw_value: Any) -> Any:
    vp = _vp()
    field_type = vp._lower(spec.get("type") or "text")
    default = spec.get("default")
    if field_type == "checkbox":
        return vp._as_bool(raw_value, vp._as_bool(default, False))

    if field_type == "number":
        minimum = spec.get("min") if isinstance(spec.get("min"), (int, float)) else None
        maximum = spec.get("max") if isinstance(spec.get("max"), (int, float)) else None
        default_num = default if isinstance(default, (int, float)) and not isinstance(default, bool) else 0
        step = spec.get("step")
        wants_int = isinstance(default, int) and not isinstance(default, bool)
        if wants_int:
            with contextlib.suppress(Exception):
                if step is not None and not float(step).is_integer():
                    wants_int = False
        if wants_int:
            min_int = int(minimum) if isinstance(minimum, (int, float)) else None
            max_int = int(maximum) if isinstance(maximum, (int, float)) else None
            return vp._as_int(raw_value, int(default_num), minimum=min_int, maximum=max_int)
        return vp._as_float(raw_value, float(default_num), minimum=minimum, maximum=maximum)

    return vp._text(raw_value if raw_value is not None else default)


def settings_fields() -> List[Dict[str, Any]]:
    vp = _vp()
    stored = vp._voice_settings()
    rows: List[Dict[str, Any]] = []
    for spec in voice_ui_setting_specs():
        if not isinstance(spec, dict):
            continue
        key = vp._text(spec.get("key"))
        if not key:
            continue
        row = dict(spec)
        row["key"] = key
        field_type = vp._lower(row.get("type") or "text")
        raw_value = stored.get(key, row.get("default"))

        if field_type in {"select", "multiselect"}:
            row["options"] = list(spec.get("options") or [])
            if key == "VOICE_OPENWAKEWORD_MODEL_SOURCE":
                with contextlib.suppress(Exception):
                    from . import openwakeword_engine

                    row["options"] = openwakeword_engine.model_source_options(current=raw_value)
            if key == "VOICE_NANOWAKEWORD_MODEL_SOURCE":
                with contextlib.suppress(Exception):
                    from . import nanowakeword_engine

                    row["options"] = nanowakeword_engine.model_source_options(current=raw_value)

        if field_type == "password":
            has_saved = bool(vp._text(stored.get(key)))
            row["value"] = ""
            if has_saved:
                existing_desc = vp._text(row.get("description"))
                keep_desc = "Leave blank to keep current saved value."
                row["description"] = f"{existing_desc} {keep_desc}".strip() if existing_desc else keep_desc
                row["placeholder"] = "Leave blank to keep current value"
        else:
            row["value"] = _voice_ui_field_value(spec, raw_value)

        rows.append(row)
    return rows


def _sections_for_groups(
    groups: List[Any],
    *,
    include_remaining: bool = False,
    remaining_exclude: set[str] | None = None,
) -> List[Dict[str, Any]]:
    vp = _vp()
    ordered_fields = settings_fields()
    by_key = {vp._text(field.get("key")): field for field in ordered_fields if isinstance(field, dict)}

    sections: List[Dict[str, Any]] = []
    used = set()
    for label, keys in groups:
        fields = []
        for key in keys:
            field = by_key.get(key)
            if not isinstance(field, dict):
                continue
            fields.append(field)
            used.add(key)
        if fields:
            sections.append({"label": label, "fields": fields})

    remaining = []
    if include_remaining:
        exclude = set(remaining_exclude or set())
        remaining = [
            field
            for field in ordered_fields
            if vp._text(field.get("key")) not in used and vp._text(field.get("key")) not in exclude
        ]
    if include_remaining and remaining:
        sections.append({"label": "Advanced", "fields": remaining})
    return sections


def settings_sections() -> List[Dict[str, Any]]:
    runtime_groups = [
        ("Debugging", ["VOICE_NATIVE_DEBUG"]),
        (
            "Tater Voice Extras",
            [
                "VOICE_CONTINUED_CHAT_ENABLED",
                "VOICE_WAKE_ARBITRATION_ENABLED",
                "VOICE_WAKE_ARBITRATION_WINDOW_MS",
                "VOICE_WAKE_ARBITRATION_BUSY_TIMEOUT_S",
                "VOICE_WAKE_ARBITRATION_SCOPE",
                "VOICE_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED",
                "VOICE_EXPERIMENTAL_PARTIAL_STT_ENABLED",
                "VOICE_EXPERIMENTAL_TTS_EARLY_START_ENABLED",
            ],
        ),
        (
            "Speech Recognition",
            [
                "VOICE_INPUT_GAIN",
                "VOICE_FASTER_WHISPER_BEAM_SIZE",
                "VOICE_FASTER_WHISPER_COMPUTE_TYPE",
                "VOICE_FASTER_WHISPER_INITIAL_PROMPT",
            ],
        ),
        (
            "Voice Activity Detection",
            [
                "VOICE_VAD_BACKEND",
                "VOICE_VAD_SILENCE_SECONDS",
                "VOICE_VAD_TIMEOUT_SECONDS",
                "VOICE_VAD_NO_SPEECH_TIMEOUT_S",
                "VOICE_SILERO_THRESHOLD",
                "VOICE_SILERO_NEG_THRESHOLD",
                "VOICE_SILERO_MIN_SPEECH_FRAMES",
                "VOICE_SILERO_MIN_SILENCE_FRAMES",
                "VOICE_VAD_MIN_SILENCE_SHORT_S",
                "VOICE_VAD_MIN_SILENCE_LONG_S",
                "VOICE_WEBRTC_VAD_AGGRESSIVENESS",
            ],
        ),
        (
            "Listening",
            [
                "VOICE_STARTUP_GATE_S",
                "VOICE_CONTINUED_CHAT_REOPEN_STARTUP_GATE_S",
                "VOICE_CONTINUED_CHAT_REOPEN_PREROLL_S",
                "VOICE_CONTINUED_CHAT_REOPEN_SILENCE_SECONDS",
                "VOICE_CONTINUED_CHAT_REOPEN_TIMEOUT_SECONDS",
                "VOICE_CONTINUED_CHAT_REOPEN_NO_SPEECH_TIMEOUT_S",
                "VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_FRAMES",
                "VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_SHORT_S",
                "VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_LONG_S",
                "VOICE_AUDIO_STALL_TIMEOUT_S",
                "VOICE_AUDIO_STALL_NO_SPEECH_TIMEOUT_S",
                "VOICE_BLANK_WAKE_TIMEOUT_S",
            ],
        ),
        (
            "Satellite Discovery",
            [
                "VOICE_DISCOVERY_ENABLED",
                "VOICE_DISCOVERY_SCAN_SECONDS",
                "VOICE_DISCOVERY_MDNS_TIMEOUT_S",
            ],
        ),
        (
            "ESPHome Connection",
            [
                "VOICE_ESPHOME_API_PORT",
                "VOICE_ESPHOME_PASSWORD",
                "VOICE_ESPHOME_NOISE_PSK",
                "VOICE_ESPHOME_CONNECT_TIMEOUT_S",
                "VOICE_ESPHOME_RETRY_SECONDS",
            ],
        ),
        (
            "External Voice Services",
            [
                "VOICE_NATIVE_WYOMING_TIMEOUT_S",
                "VOICE_WYOMING_STT_QUEUE_CHUNKS",
                "VOICE_WYOMING_STT_QUEUE_DROP_LIMIT",
            ],
        ),
    ]
    return _sections_for_groups(
        runtime_groups,
        include_remaining=True,
        remaining_exclude=VOICE_MODEL_SETTING_KEYS,
    )


def model_settings_sections() -> List[Dict[str, Any]]:
    return _sections_for_groups(VOICE_MODEL_SETTING_GROUPS, include_remaining=False)


def settings_item_form() -> Dict[str, Any]:
    return {
        "id": "voice_settings",
        "group": "settings",
        "title": "Voice Pipeline Settings",
        "subtitle": "Tune ESPHome and runtime behavior here. Shared STT/TTS model choices now live in Tater Settings under Models.",
        "sections": list(settings_sections()),
        "save_action": "voice_settings_save",
        "save_label": "Save Settings",
        "reset_action": "voice_settings_reset_defaults",
        "reset_label": "Restore Default Settings",
        "reset_confirm": "Restore ESPHome voice settings to defaults? This also clears the saved ESPHome API password and Noise PSK.",
        "settings_title": "Voice Pipeline Settings",
        "fields_dropdown": False,
        "sections_in_dropdown": False,
        "remove_action": "",
    }


def satellite_item_forms(status: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = esphome_ui_helpers.satellite_item_forms(status)
    return rows if isinstance(rows, list) else []


def save_settings_values(values: Dict[str, Any]) -> Dict[str, Any]:
    vp = _vp()
    incoming = values if isinstance(values, dict) else {}
    specs = _voice_ui_spec_map()
    current = vp._voice_settings()
    mapping: Dict[str, str] = {}
    changed_keys: List[str] = []
    openwakeword_model_settings_touched = False

    for key, spec in specs.items():
        if key not in incoming:
            continue
        if key in {"VOICE_OPENWAKEWORD_MODEL_SOURCE", "VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK"}:
            openwakeword_model_settings_touched = True
        field_type = vp._lower(spec.get("type") or "text")
        raw_value = incoming.get(key)

        if field_type == "password":
            token = vp._text(raw_value)
            if not token:
                continue
            normalized = token
        elif field_type == "checkbox":
            normalized = "true" if vp._as_bool(raw_value, False) else "false"
        elif field_type == "number":
            coerced = _voice_ui_field_value(spec, raw_value)
            if isinstance(coerced, float) and coerced.is_integer():
                normalized = str(int(coerced))
            else:
                normalized = str(coerced)
        elif field_type == "select":
            normalized = vp._text(raw_value)
            allowed = []
            for option in list(spec.get("options") or []):
                if isinstance(option, dict):
                    allowed.append(vp._text(option.get("value") or option.get("id") or option.get("key")))
                else:
                    allowed.append(vp._text(option))
            allowed = [item for item in allowed if item]
            if allowed and normalized not in allowed:
                normalized = vp._text(current.get(key)) or vp._text(spec.get("default"))
        else:
            normalized = vp._text(raw_value)

        if key == "VOICE_OPENWAKEWORD_MODEL_SOURCE":
            try:
                from . import openwakeword_engine

                normalized, _inferred_openwakeword_framework = openwakeword_engine.normalize_model_source(
                    normalized,
                    framework=(
                        incoming.get("VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK")
                        or current.get("VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK")
                    ),
                    copy_external=True,
                )
            except Exception as exc:
                raise ValueError(str(exc) or "Invalid openWakeWord model source.") from exc

        if key == "VOICE_NANOWAKEWORD_MODEL_SOURCE":
            try:
                from . import nanowakeword_engine

                normalized = nanowakeword_engine.normalize_model_source(normalized, copy_external=True)
            except Exception as exc:
                raise ValueError(str(exc) or "Invalid NanoWakeWord model source.") from exc

        old = vp._text(current.get(key))
        if normalized != old:
            mapping[key] = normalized
            changed_keys.append(key)

    def _set_normalized_mapping(key: str, normalized: str) -> None:
        old = vp._text(current.get(key))
        if normalized != old:
            mapping[key] = normalized
            if key not in changed_keys:
                changed_keys.append(key)
            return
        mapping.pop(key, None)
        while key in changed_keys:
            changed_keys.remove(key)

    if openwakeword_model_settings_touched:
        try:
            from . import openwakeword_engine

            normalized_source, inferred_openwakeword_framework = openwakeword_engine.normalize_model_source(
                mapping.get("VOICE_OPENWAKEWORD_MODEL_SOURCE")
                or vp._text(current.get("VOICE_OPENWAKEWORD_MODEL_SOURCE")),
                framework=(
                    mapping.get("VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK")
                    or vp._text(current.get("VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK"))
                ),
                copy_external=True,
            )
        except Exception as exc:
            raise ValueError(str(exc) or "Invalid openWakeWord model source.") from exc
        _set_normalized_mapping("VOICE_OPENWAKEWORD_MODEL_SOURCE", normalized_source)
        if inferred_openwakeword_framework:
            _set_normalized_mapping("VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK", inferred_openwakeword_framework)

    if mapping:
        redis_client.hset(settings_hash_key(), mapping=mapping)

    return {
        "updated_count": len(changed_keys),
        "changed_keys": changed_keys,
        "restart_required": False,
    }


def runtime_setting_keys() -> List[str]:
    vp = _vp()
    keys: List[str] = []
    seen = set()
    for spec in voice_ui_setting_specs():
        if not isinstance(spec, dict):
            continue
        key = vp._text(spec.get("key"))
        if not key or key in seen or key in VOICE_MODEL_SETTING_KEYS:
            continue
        seen.add(key)
        keys.append(key)
    return keys


def reset_settings_defaults() -> Dict[str, Any]:
    keys = runtime_setting_keys()
    delete_keys = sorted(set(keys) | LEGACY_RUNTIME_SETTING_KEYS)
    current = _vp()._voice_settings()
    changed_keys = [key for key in delete_keys if key in current]

    if delete_keys:
        redis_client.hdel(settings_hash_key(), *delete_keys)

    return {
        "updated_count": len(changed_keys),
        "changed_keys": changed_keys,
        "reset_keys": keys,
        "restart_required": False,
    }
