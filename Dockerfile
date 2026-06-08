# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.11-slim
ARG NVIDIA_RUNTIME_IMAGE=nvidia/cuda:12.8.0-cudnn-runtime-ubuntu22.04


FROM ${PYTHON_IMAGE} AS builder

ARG TORCH_VERSION=2.7.1
ARG ONNXRUNTIME_VERSION=1.22.0

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app

RUN python -m venv /opt/venv \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        git \
        libpq-dev \
        libolm-dev \
        libffi-dev \
        pkg-config \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel

RUN python -m pip install --index-url https://download.pytorch.org/whl/cpu \
    "torch==${TORCH_VERSION}"

RUN python -m pip install "llama-cpp-python>=0.3.23"

RUN python -m pip install "onnxruntime==${ONNXRUNTIME_VERSION}"

COPY requirements.txt .

RUN grep -Ev '^[[:space:]]*(torch|llama-cpp-python|onnxruntime)([[:space:]=<>!~]|$)' requirements.txt > /tmp/requirements.runtime.txt \
    && python -m pip install -r /tmp/requirements.runtime.txt \
    && rm -f /tmp/requirements.runtime.txt


FROM ${PYTHON_IMAGE} AS runtime-base

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    HTMLUI_PORT=8501

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        wget \
        ffmpeg \
        libpq5 \
        libolm3 \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
COPY . .

EXPOSE 8501

CMD ["sh", "run_ui.sh"]


FROM ${NVIDIA_RUNTIME_IMAGE} AS runtime-nvidia

ARG TORCH_VERSION=2.7.1
ARG TORCHAUDIO_VERSION=2.7.1
ARG TORCHVISION_VERSION=0.22.1
ARG LLAMA_CPP_CUDA_WHEEL=cu124
ARG ONNXRUNTIME_GPU_WHEEL=https://files.pythonhosted.org/packages/dc/0f/696b4f94a282952239ffed39db78cb17a00ad993acd929cfac010a09759b/onnxruntime_gpu-1.26.0-cp311-cp311-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    HTMLUI_PORT=8501 \
    TATER_SPEECH_ACCELERATION=auto \
    TATER_FASTER_WHISPER_COMPUTE_TYPE=auto \
    TATER_LLAMA_CPP_N_GPU_LAYERS=auto

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        ffmpeg \
        git \
        libgomp1 \
        libolm3 \
        libpq5 \
        libsndfile1 \
        python3.11 \
        python3.11-venv \
        wget \
    && python3.11 -m venv /opt/venv \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel

RUN python -m pip install --index-url https://download.pytorch.org/whl/cu128 \
    "torch==${TORCH_VERSION}" \
    "torchaudio==${TORCHAUDIO_VERSION}" \
    "torchvision==${TORCHVISION_VERSION}"

RUN python -m pip install \
    --extra-index-url "https://abetlen.github.io/llama-cpp-python/whl/${LLAMA_CPP_CUDA_WHEEL}" \
    "llama-cpp-python>=0.3.23"

COPY requirements.txt .

RUN grep -Ev '^[[:space:]]*(torch|torchaudio|torchvision|llama-cpp-python|onnxruntime)([[:space:]=<>!~]|$)' requirements.txt > /tmp/requirements.nvidia.txt \
    && python -m pip install -r /tmp/requirements.nvidia.txt \
    && python -m pip install "${ONNXRUNTIME_GPU_WHEEL}" \
    && rm -f /tmp/requirements.nvidia.txt

RUN python -c "from importlib import metadata; import ctranslate2, onnxruntime, torch, torchaudio; print('torch', torch.__version__, 'cuda', torch.version.cuda, 'torchaudio', torchaudio.__version__, 'onnxruntime', onnxruntime.__version__, 'ctranslate2', ctranslate2.__version__, 'llama_cpp_python', metadata.version('llama-cpp-python'))"

COPY . .

RUN mkdir -p /app/.runtime

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os, urllib.request; port = os.environ.get('HTMLUI_PORT', '8501'); urllib.request.urlopen(f'http://127.0.0.1:{port}/api/health', timeout=3)" || exit 1

CMD ["sh", "run_ui.sh"]


FROM runtime-base AS runtime
