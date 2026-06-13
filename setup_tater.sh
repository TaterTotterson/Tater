#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd "$(dirname "$0")" && pwd -P)"
cd "${SCRIPT_DIR}"

VENV_DIR="${TATER_VENV_DIR:-.venv}"
RUNTIME_DIR="${TATER_RUNTIME_DIR:-.runtime}"
PROFILE_FILE="${TATER_SETUP_PROFILE_FILE:-${RUNTIME_DIR}/setup_profile}"
PROFILE_ENV="${TATER_PROFILE_ENV:-${RUNTIME_DIR}/tater_profile.env}"
REQUIREMENTS_FILE="${TATER_REQUIREMENTS_FILE:-requirements.txt}"
LLAMA_CPP_REPO="${TATER_LLAMA_CPP_REPO:-https://github.com/ggml-org/llama.cpp.git}"
LLAMA_CPP_REF="${TATER_LLAMA_CPP_REF:-master}"
LLAMA_CPP_DIR="${TATER_LLAMA_CPP_DIR:-${RUNTIME_DIR}/llama.cpp}"
LLAMA_CPP_SERVER_BIN="${TATER_LLAMA_CPP_SERVER_BIN:-${LLAMA_CPP_DIR}/build/bin/llama-server}"

RED=""
GREEN=""
YELLOW=""
BLUE=""
BOLD=""
RESET=""

if [ -t 1 ]; then
  RED="$(printf '\033[31m')"
  GREEN="$(printf '\033[32m')"
  YELLOW="$(printf '\033[33m')"
  BLUE="$(printf '\033[36m')"
  BOLD="$(printf '\033[1m')"
  RESET="$(printf '\033[0m')"
fi

say() {
  printf '%s\n' "$*"
}

info() {
  printf '%s==>%s %s\n' "${BLUE}" "${RESET}" "$*"
}

ok() {
  printf '%sOK%s  %s\n' "${GREEN}" "${RESET}" "$*"
}

warn() {
  printf '%sWARN%s %s\n' "${YELLOW}" "${RESET}" "$*"
}

fail() {
  printf '%sERROR%s %s\n' "${RED}" "${RESET}" "$*" >&2
  exit 1
}

banner() {
  if command -v clear >/dev/null 2>&1 && [ -t 1 ]; then
    clear
  fi
  say "============================================================"
  say "                      TATER SETUP"
  say "============================================================"
  say "Choose the runtime profile for this machine. Setup creates"
  say "a local ${VENV_DIR} and writes ${PROFILE_ENV}."
  say ""
}

usage() {
  say "Usage: sh setup_tater.sh [cpu|macos|nvidia|rocm|jetson|thor]"
  say ""
  say "Profiles:"
  say "  cpu     Local CPU-first install for most systems."
  say "  macos   Native Apple Silicon/macOS install with MLX and MPS where supported."
  say "  nvidia  Native desktop/server NVIDIA install for amd64 CUDA PCs."
  say "  rocm    Native AMD ROCm install for Radeon / Strix Halo systems."
  say "  jetson  Native Jetson install that uses JetPack/system AI packages."
  say "  thor    Native Jetson Thor install for JetPack 7 / CUDA 13 systems."
}

choose_profile() {
  if [ "${1:-}" ]; then
    SELECTED_PROFILE="$1"
    return
  fi

  banner
  say "  1) CPU"
  say "     Safe default. Works on normal Linux and generic ARM hosts."
  say ""
  say "  2) macOS Apple Silicon"
  say "     Native Mac setup with MLX Whisper and PyTorch/MPS Kokoro."
  say ""
  say "  3) NVIDIA desktop/server"
  say "     Native amd64 CUDA setup for RTX/GTX machines."
  say ""
  say "  4) AMD ROCm / Strix Halo"
  say "     Native AMD GPU setup for ROCm-capable Linux systems."
  say ""
  say "  5) Jetson"
  say "     Native ARM64 setup for Jetson Orin / JetPack systems."
  say ""
  say "  6) Jetson Thor"
  say "     Native ARM64 setup for Thor / JetPack 7 systems."
  say ""
  printf "Select profile [1-6]: "
  read -r choice

  case "${choice}" in
    1) SELECTED_PROFILE="cpu" ;;
    2) SELECTED_PROFILE="macos" ;;
    3) SELECTED_PROFILE="nvidia" ;;
    4) SELECTED_PROFILE="rocm" ;;
    5) SELECTED_PROFILE="jetson" ;;
    6) SELECTED_PROFILE="thor" ;;
    *) fail "Unknown profile selection: ${choice}" ;;
  esac
}

normalize_profile() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    cpu|default|local) printf '%s' "cpu" ;;
    mac|macos|darwin|apple|apple-silicon|apple_silicon) printf '%s' "macos" ;;
    nvidia|cuda|gpu) printf '%s' "nvidia" ;;
    amd|rocm|amd-rocm|amd_rocm|radeon|strix|strix-halo|strix_halo) printf '%s' "rocm" ;;
    jetson|orin) printf '%s' "jetson" ;;
    thor|jetson-thor|jetson_thor) printf '%s' "thor" ;;
    -h|--help|help) usage; exit 0 ;;
    *) fail "Unknown setup profile: $1" ;;
  esac
}

find_python() {
  if [ "${PYTHON:-}" ] && command -v "${PYTHON}" >/dev/null 2>&1; then
    printf '%s' "${PYTHON}"
    return
  fi
  for candidate in \
    python3.11 \
    /opt/homebrew/opt/python@3.11/bin/python3.11 \
    /opt/homebrew/bin/python3.11 \
    /usr/local/opt/python@3.11/bin/python3.11 \
    /usr/local/bin/python3.11 \
    python3.12 \
    /opt/homebrew/opt/python@3.12/bin/python3.12 \
    /opt/homebrew/bin/python3.12 \
    /usr/local/opt/python@3.12/bin/python3.12 \
    /usr/local/bin/python3.12 \
    python3.13 \
    /opt/homebrew/opt/python@3.13/bin/python3.13 \
    /opt/homebrew/bin/python3.13 \
    /usr/local/opt/python@3.13/bin/python3.13 \
    /usr/local/bin/python3.13 \
    python3 \
    python
  do
    if command -v "${candidate}" >/dev/null 2>&1; then
      printf '%s' "${candidate}"
      return
    fi
  done
  fail "Python was not found. Install Python 3.11 or newer, then rerun setup."
}

python_version() {
  "$1" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
}

confirm() {
  prompt="$1"
  default="${2:-n}"
  if [ ! -t 0 ]; then
    [ "${default}" = "y" ]
    return
  fi
  printf "%s " "${prompt}"
  read -r answer
  answer="$(printf '%s' "${answer:-$default}" | tr '[:upper:]' '[:lower:]')"
  case "${answer}" in
    y|yes) return 0 ;;
    *) return 1 ;;
  esac
}

truthy_env() {
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

ensure_venv() {
  profile="$1"
  python_bin="$2"
  existing_profile=""
  if [ -f "${PROFILE_FILE}" ]; then
    existing_profile="$(cat "${PROFILE_FILE}" 2>/dev/null || true)"
  fi

  if [ -x "${VENV_DIR}/bin/python" ] && [ "${existing_profile}" != "${profile}" ]; then
    warn "Existing ${VENV_DIR} was prepared for '${existing_profile:-unknown}', not '${profile}'."
    if truthy_env "${TATER_SETUP_REBUILD:-}"; then
      info "Rebuilding ${VENV_DIR} for ${profile}"
      rm -rf "${VENV_DIR}"
    elif confirm "Rebuild ${VENV_DIR} for ${profile}? [y/N]" "n"; then
      rm -rf "${VENV_DIR}"
    else
      fail "Setup cancelled. Re-run with the matching profile or rebuild the venv."
    fi
  fi

  if [ ! -x "${VENV_DIR}/bin/python" ]; then
    info "Creating ${VENV_DIR}"
    mkdir -p "$(dirname "${VENV_DIR}")"
    if [ "${profile}" = "jetson" ] || [ "${profile}" = "thor" ]; then
      "${python_bin}" -m venv --system-site-packages "${VENV_DIR}"
    else
      "${python_bin}" -m venv "${VENV_DIR}"
    fi
  else
    ok "Using existing ${VENV_DIR}"
  fi
}

filtered_requirements() {
  output_file="$1"
  awk '
    /^[[:space:]]*($|#)/ { print; next }
    {
      line = $0
      lower = tolower(line)
      if (lower ~ /^[[:space:]]*(torch|torchaudio|torchvision)([[:space:]]|[=<>!~]|$)/) {
        next
      }
      print line
    }
  ' "${REQUIREMENTS_FILE}" > "${output_file}"
}

filtered_nvidia_requirements() {
  output_file="$1"
  awk '
    /^[[:space:]]*($|#)/ { print; next }
    {
      line = $0
      lower = tolower(line)
      if (lower ~ /^[[:space:]]*(torch|torchaudio|torchvision)([[:space:]]|[=<>!~]|$)/) {
        next
      }
      print line
    }
  ' "${REQUIREMENTS_FILE}" > "${output_file}"
}

filtered_macos_requirements() {
  output_file="$1"
  awk '
    /^[[:space:]]*($|#)/ { print; next }
    {
      line = $0
      lower = tolower(line)
      if (lower ~ /^[[:space:]]*pykokoro([[:space:]]|[=<>!~]|$)/) {
        next
      }
      print line
    }
  ' "${REQUIREMENTS_FILE}" > "${output_file}"
}

install_base() {
  venv_python="$1"
  info "Upgrading pip tooling"
  "${venv_python}" -m pip install --upgrade pip setuptools wheel
}

install_cpu() {
  venv_python="$1"
  info "Installing Tater dependencies"
  "${venv_python}" -m pip install -r "${REQUIREMENTS_FILE}"
  install_llama_cpp_native cpu
}

check_llama_cpp_native() {
  server_bin="${1:-${LLAMA_CPP_SERVER_BIN}}"
  [ -x "${server_bin}" ] || return 1
  "${server_bin}" --version >/dev/null 2>&1
}

llama_cpp_native_cmake_args() {
  profile="$1"
  if [ "${TATER_LLAMA_CPP_CMAKE_ARGS:-}" ]; then
    printf '%s' "${TATER_LLAMA_CPP_CMAKE_ARGS}"
    return
  fi
  case "${profile}" in
    macos)
      if [ "$(uname -s 2>/dev/null || printf unknown)" = "Darwin" ]; then
        printf '%s' "-DGGML_METAL=on"
      fi
      ;;
    nvidia|jetson|thor)
      printf '%s' "-DGGML_CUDA=on"
      ;;
    rocm)
      printf '%s' "-DGGML_HIP=on"
      ;;
    *)
      printf '%s' ""
      ;;
  esac
}

llama_cpp_cuda_stub_dir() {
  profile="$1"
  case "${profile}" in
    nvidia|jetson|thor)
      ;;
    *)
      return
      ;;
  esac
  candidates=""
  if [ "${TATER_LLAMA_CPP_CUDA_STUB_DIR:-}" ]; then
    candidates="${candidates} ${TATER_LLAMA_CPP_CUDA_STUB_DIR}"
  fi
  if [ "${CUDA_HOME:-}" ]; then
    candidates="${candidates} ${CUDA_HOME}/lib64/stubs ${CUDA_HOME}/targets/x86_64-linux/lib/stubs ${CUDA_HOME}/targets/aarch64-linux/lib/stubs"
  fi
  if [ "${CUDA_PATH:-}" ]; then
    candidates="${candidates} ${CUDA_PATH}/lib64/stubs ${CUDA_PATH}/targets/x86_64-linux/lib/stubs ${CUDA_PATH}/targets/aarch64-linux/lib/stubs"
  fi
  candidates="${candidates} /usr/local/cuda/lib64/stubs /usr/local/cuda/targets/x86_64-linux/lib/stubs /usr/local/cuda/targets/aarch64-linux/lib/stubs"
  for candidate in ${candidates}; do
    if [ -f "${candidate}/libcuda.so" ]; then
      if [ -e "${candidate}/libcuda.so.1" ]; then
        printf '%s' "${candidate}"
        return
      fi
      if [ -w "${candidate}" ] && ln -sf libcuda.so "${candidate}/libcuda.so.1" 2>/dev/null; then
        printf '%s' "${candidate}"
        return
      fi
      runtime_stub_dir="${RUNTIME_DIR}/cuda-stubs"
      mkdir -p "${runtime_stub_dir}"
      ln -sf "${candidate}/libcuda.so" "${runtime_stub_dir}/libcuda.so"
      ln -sf "${candidate}/libcuda.so" "${runtime_stub_dir}/libcuda.so.1"
      printf '%s' "${runtime_stub_dir}"
      return
    fi
  done
}

install_llama_cpp_native() {
  profile="$1"
  if [ "${TATER_SETUP_LLAMA_CPP_NATIVE:-1}" = "0" ]; then
    warn "Skipping native llama.cpp build because TATER_SETUP_LLAMA_CPP_NATIVE=0."
    return
  fi
  if check_llama_cpp_native "${LLAMA_CPP_SERVER_BIN}"; then
    ok "Using native llama.cpp server at ${LLAMA_CPP_SERVER_BIN}"
    return
  fi
  command -v git >/dev/null 2>&1 || { warn "git was not found; skipping native llama.cpp build."; return; }
  command -v cmake >/dev/null 2>&1 || { warn "cmake was not found; install cmake and rerun setup for native llama.cpp."; return; }
  mkdir -p "${RUNTIME_DIR}"
  if [ ! -d "${LLAMA_CPP_DIR}/.git" ]; then
    info "Cloning native llama.cpp runtime"
    git clone --depth 1 --branch "${LLAMA_CPP_REF}" "${LLAMA_CPP_REPO}" "${LLAMA_CPP_DIR}" || { warn "Could not clone llama.cpp."; return; }
  else
    info "Updating native llama.cpp runtime"
    git -C "${LLAMA_CPP_DIR}" fetch --depth 1 origin "${LLAMA_CPP_REF}" || warn "Could not fetch llama.cpp ${LLAMA_CPP_REF}; using existing checkout."
    git -C "${LLAMA_CPP_DIR}" checkout FETCH_HEAD >/dev/null 2>&1 || true
  fi
  cmake_args="$(llama_cpp_native_cmake_args "${profile}")"
  cuda_stub_dir="$(llama_cpp_cuda_stub_dir "${profile}")"
  info "Building native llama-server${cmake_args:+ (${cmake_args})}"
  if [ "${cuda_stub_dir}" ]; then
    info "Using CUDA driver stubs for llama.cpp link: ${cuda_stub_dir}"
    # shellcheck disable=SC2086
    cmake -S "${LLAMA_CPP_DIR}" -B "${LLAMA_CPP_DIR}/build" -DCMAKE_BUILD_TYPE=Release ${cmake_args} \
      "-DCMAKE_EXE_LINKER_FLAGS=-L${cuda_stub_dir} -Wl,-rpath-link,${cuda_stub_dir}" \
      "-DCMAKE_SHARED_LINKER_FLAGS=-L${cuda_stub_dir} -Wl,-rpath-link,${cuda_stub_dir}" || { warn "llama.cpp configure failed."; return; }
  else
    # shellcheck disable=SC2086
    cmake -S "${LLAMA_CPP_DIR}" -B "${LLAMA_CPP_DIR}/build" -DCMAKE_BUILD_TYPE=Release ${cmake_args} || { warn "llama.cpp configure failed."; return; }
  fi
  cmake --build "${LLAMA_CPP_DIR}/build" --config Release --target llama-server -j "${TATER_LLAMA_CPP_BUILD_JOBS:-4}" || { warn "llama-server build failed."; return; }
  if check_llama_cpp_native "${LLAMA_CPP_SERVER_BIN}"; then
    ok "Built native llama.cpp server at ${LLAMA_CPP_SERVER_BIN}"
  else
    warn "llama-server build finished, but ${LLAMA_CPP_SERVER_BIN} was not executable."
  fi
}

install_mlx_engine_checkout() {
  if [ "${TATER_SETUP_MLX_ENGINE:-1}" = "0" ]; then
    warn "Skipping MLX engine checkout because TATER_SETUP_MLX_ENGINE=0."
    return
  fi
  if ! command -v git >/dev/null 2>&1; then
    warn "git was not found; skipping optional MLX engine checkout."
    return
  fi
  mkdir -p "${RUNTIME_DIR}"
  engine_dir="${RUNTIME_DIR}/mlx-engine"
  if [ -d "${engine_dir}/mlx_engine" ]; then
    ok "Using existing ${engine_dir}"
    if [ -d "${engine_dir}/.git" ]; then
      info "Updating optional MLX engine checkout"
      git -C "${engine_dir}" pull --ff-only || warn "Could not update ${engine_dir}; using the existing checkout."
    fi
    return
  fi
  if [ -e "${engine_dir}" ]; then
    warn "${engine_dir} exists but does not look like an mlx-engine checkout. Set TATER_MLX_ENGINE_PATH or remove it and rerun setup."
    return
  fi
  info "Cloning optional MLX engine runtime"
  git clone --depth 1 https://github.com/lmstudio-ai/mlx-engine.git "${engine_dir}" || warn "Could not clone mlx-engine; Tater will use mlx-lm/mlx-vlm directly."
}

install_macos() {
  venv_python="$1"
  tmp_req="$(mktemp "${TMPDIR:-/tmp}/tater-requirements-macos.XXXXXX")"
  trap 'rm -f "${tmp_req}"' EXIT
  filtered_macos_requirements "${tmp_req}"
  is_apple_silicon="0"
  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Darwin" ]; then
    warn "macOS profile selected on a non-macOS host."
  else
    arch="$(uname -m 2>/dev/null || printf unknown)"
    if [ "${arch}" = "arm64" ]; then
      ok "Detected Apple Silicon (${arch})"
      is_apple_silicon="1"
    else
      warn "Detected macOS ${arch}. This profile is tuned for Apple Silicon but may still run CPU-first."
    fi
    if ! command -v brew >/dev/null 2>&1; then
      warn "Homebrew was not found. If installs fail, install Homebrew packages: ffmpeg libolm pkg-config cmake."
    else
      warn "If native package builds fail, run: brew install ffmpeg libolm pkg-config cmake"
    fi
  fi
  info "Installing Tater dependencies for macOS"
  "${venv_python}" -m pip install -r "${tmp_req}"
  install_llama_cpp_native macos
  info "Installing Apple-native speech extras"
  if ! "${venv_python}" -m pip install mlx-whisper kokoro; then
    warn "Apple-native speech extras failed to install. Tater will still run with Faster Whisper/Kokoro CPU fallbacks."
  fi
  if [ "${is_apple_silicon}" = "1" ]; then
    install_mlx_engine_checkout
  fi
  rm -f "${tmp_req}"
  trap - EXIT
}

install_nvidia() {
  venv_python="$1"
  tmp_req="$(mktemp "${TMPDIR:-/tmp}/tater-requirements-nvidia.XXXXXX")"
  trap 'rm -f "${tmp_req}"' EXIT
  filtered_nvidia_requirements "${tmp_req}"

  info "Installing NVIDIA PyTorch CUDA wheels"
  "${venv_python}" -m pip install --index-url https://download.pytorch.org/whl/cu128 torch torchaudio
  info "Installing CUDA runtime Python packages"
  "${venv_python}" -m pip install "nvidia-cublas-cu12" "nvidia-cudnn-cu12==9.*"
  info "Installing Tater dependencies"
  "${venv_python}" -m pip install -r "${tmp_req}"
  install_llama_cpp_native nvidia
  info "Switching ONNX Runtime to GPU build"
  "${venv_python}" -m pip uninstall -y onnxruntime >/dev/null 2>&1 || true
  "${venv_python}" -m pip install onnxruntime-gpu
  rm -f "${tmp_req}"
  trap - EXIT
}

install_rocm() {
  venv_python="$1"
  tmp_req="$(mktemp "${TMPDIR:-/tmp}/tater-requirements-rocm.XXXXXX")"
  trap 'rm -f "${tmp_req}"' EXIT
  filtered_requirements "${tmp_req}"

  rocm_index="${TATER_ROCM_PYTORCH_INDEX_URL:-https://download.pytorch.org/whl/rocm6.4}"
  warn "AMD ROCm support is Linux-only and depends on the ROCm runtime installed for your GPU/APU."
  warn "Strix Halo systems may need newer AMD ROCm wheels; override with TATER_ROCM_PYTORCH_INDEX_URL if needed."
  info "Installing AMD ROCm PyTorch wheels from ${rocm_index}"
  "${venv_python}" -m pip install --index-url "${rocm_index}" torch torchaudio
  info "Installing Tater dependencies"
  "${venv_python}" -m pip install -r "${tmp_req}"
  install_llama_cpp_native rocm
  info "Installing PyTorch Kokoro runtime"
  if ! "${venv_python}" -m pip install kokoro; then
    warn "Kokoro PyTorch failed to install. Tater will still run with CPU/ONNX TTS fallbacks."
  fi
  rm -f "${tmp_req}"
  trap - EXIT
}

install_jetson_like() {
  venv_python="$1"
  profile="$2"
  tmp_req="$(mktemp "${TMPDIR:-/tmp}/tater-requirements.XXXXXX")"
  trap 'rm -f "${tmp_req}"' EXIT

  filtered_requirements "${tmp_req}"
  info "Installing Tater dependencies without replacing JetPack PyTorch"
  "${venv_python}" -m pip install -r "${tmp_req}"
  install_llama_cpp_native "${profile}"

  if ! "${venv_python}" -c 'import torch' >/dev/null 2>&1; then
    warn "PyTorch is not importable in ${VENV_DIR}."
    warn "Install NVIDIA's JetPack-compatible PyTorch for ${profile}, then rerun setup if GPU speech is needed."
  fi
}

write_profile_env() {
  profile="$1"
  mkdir -p "${RUNTIME_DIR}"

  speech_acceleration="cpu"
  compute_type="auto"
  torch_mps_fallback=""
  nvidia_site_packages=""
  case "${profile}" in
    cpu)
      speech_acceleration="cpu"
      ;;
    macos)
      speech_acceleration="auto"
      torch_mps_fallback="1"
      ;;
    nvidia|jetson|thor)
      speech_acceleration="auto"
      ;;
    rocm)
      speech_acceleration="rocm"
      ;;
  esac
  if [ "${profile}" = "nvidia" ] && [ -x "${VENV_DIR}/bin/python" ]; then
    nvidia_site_packages="$("${VENV_DIR}/bin/python" -c 'import site; paths = site.getsitepackages(); print(paths[0] if paths else "")' 2>/dev/null || true)"
  fi

  {
    say "# Generated by setup_tater.sh"
    say "export TATER_SETUP_PROFILE=\"\${TATER_SETUP_PROFILE:-${profile}}\""
    say "export TATER_SPEECH_ACCELERATION=\"\${TATER_SPEECH_ACCELERATION:-${speech_acceleration}}\""
    say "export TATER_FASTER_WHISPER_COMPUTE_TYPE=\"\${TATER_FASTER_WHISPER_COMPUTE_TYPE:-${compute_type}}\""
    say "export TATER_KOKORO_ENGINE=\"\${TATER_KOKORO_ENGINE:-auto}\""
    say "export TATER_LLAMA_CPP_SERVER_BIN=\"\${TATER_LLAMA_CPP_SERVER_BIN:-${LLAMA_CPP_SERVER_BIN}}\""
    if [ "${torch_mps_fallback}" ]; then
      say "export PYTORCH_ENABLE_MPS_FALLBACK=\"\${PYTORCH_ENABLE_MPS_FALLBACK:-${torch_mps_fallback}}\""
    fi
    if [ "${profile}" = "nvidia" ]; then
      say "export TATER_LLAMA_CPP_N_GPU_LAYERS=\"\${TATER_LLAMA_CPP_N_GPU_LAYERS:-auto}\""
      if [ "${nvidia_site_packages}" ]; then
        say "export LD_LIBRARY_PATH=\"${nvidia_site_packages}/nvidia/cublas/lib:${nvidia_site_packages}/nvidia/cuda_runtime/lib:${nvidia_site_packages}/nvidia/cuda_nvrtc/lib:${nvidia_site_packages}/nvidia/cudnn/lib:${nvidia_site_packages}/nvidia/curand/lib:${nvidia_site_packages}/nvidia/cusolver/lib:${nvidia_site_packages}/nvidia/cusparse/lib:${nvidia_site_packages}/nvidia/nvjitlink/lib:\${LD_LIBRARY_PATH:-}\""
      fi
    fi
  } > "${PROFILE_ENV}"

  printf '%s\n' "${profile}" > "${PROFILE_FILE}"
}

verify_install() {
  venv_python="$1"
  profile="$2"
  info "Checking installed runtime"
  TATER_LLAMA_CPP_SERVER_BIN="${LLAMA_CPP_SERVER_BIN}" "${venv_python}" - <<'PY'
import importlib.util

required = ["fastapi", "uvicorn", "redis", "redislite", "aioesphomeapi"]
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    raise SystemExit("Missing required packages: " + ", ".join(missing))

print("core imports ok")

try:
    import torch
    cuda = bool(getattr(torch, "cuda", None) and torch.cuda.is_available())
    hip = getattr(getattr(torch, "version", None), "hip", None)
    print(f"torch {getattr(torch, '__version__', 'unknown')} cuda={cuda}")
    print(f"torch_rocm={bool(cuda and hip)} hip={hip or '-'}")
    mps = bool(getattr(getattr(torch, "backends", None), "mps", None) and torch.backends.mps.is_available())
    print(f"torch_mps={mps}")
except Exception as exc:
    print(f"torch unavailable: {exc}")

try:
    import ctranslate2
    count = int(getattr(ctranslate2, "get_cuda_device_count")())
    print(f"ctranslate2_cuda_devices={count}")
except Exception as exc:
    print(f"ctranslate2 unavailable: {exc}")

try:
    import onnxruntime as ort
    print("onnxruntime providers=" + ",".join(ort.get_available_providers()))
except Exception as exc:
    print(f"onnxruntime unavailable: {exc}")

import os
import subprocess

server_bin = os.getenv("TATER_LLAMA_CPP_SERVER_BIN", "")
if server_bin:
    try:
        completed = subprocess.run([server_bin, "--version"], text=True, capture_output=True, timeout=10)
        output = " ".join(((completed.stdout or "") + " " + (completed.stderr or "")).split())
        print(f"llama_server={server_bin} {output}")
    except Exception as exc:
        print(f"llama_server unavailable: {exc}")
else:
    print("llama_server unavailable: TATER_LLAMA_CPP_SERVER_BIN is not set")

for name in ("mlx_whisper", "kokoro"):
    try:
        __import__(name)
        print(f"{name}=available")
    except Exception as exc:
        print(f"{name}=unavailable: {exc}")
PY
  ok "Profile '${profile}' is ready"
}

main() {
  [ -f "${REQUIREMENTS_FILE}" ] || fail "Run setup from the Tater repository root."

  case "${1:-}" in
    -h|--help|help)
      usage
      exit 0
      ;;
  esac

  SELECTED_PROFILE=""
  choose_profile "${1:-}"
  profile="$(normalize_profile "${SELECTED_PROFILE}")"
  banner
  info "Selected profile: ${BOLD}${profile}${RESET}"

  python_bin="$(find_python)"
  version="$(python_version "${python_bin}")"
  case "${version}" in
    3.11|3.12|3.13)
      ok "Using ${python_bin} ${version}"
      ;;
    *)
      warn "Detected Python ${version}. Tater is tested most heavily on Python 3.11."
      ;;
  esac

  if [ "${profile}" = "thor" ]; then
    warn "Thor should use JetPack 7 / CUDA 13 packages from NVIDIA. This script will not install system CUDA."
  elif [ "${profile}" = "jetson" ]; then
    warn "Jetson should use JetPack packages from NVIDIA. This script will not install system CUDA."
  elif [ "${profile}" = "nvidia" ]; then
    warn "NVIDIA profile is for native amd64 desktop/server CUDA systems, not Jetson."
  elif [ "${profile}" = "rocm" ]; then
    warn "AMD ROCm profile is for native Linux Radeon / Strix Halo systems with ROCm installed."
  elif [ "${profile}" = "macos" ]; then
    warn "macOS profile can use Apple Metal/MPS for PyTorch-backed SpeechBrain and Kokoro, plus MLX Whisper for STT."
  fi

  ensure_venv "${profile}" "${python_bin}"
  venv_python="${VENV_DIR}/bin/python"
  install_base "${venv_python}"

  case "${profile}" in
    cpu) install_cpu "${venv_python}" ;;
    macos) install_macos "${venv_python}" ;;
    nvidia) install_nvidia "${venv_python}" ;;
    rocm) install_rocm "${venv_python}" ;;
    jetson|thor) install_jetson_like "${venv_python}" "${profile}" ;;
  esac

  write_profile_env "${profile}"
  verify_install "${venv_python}" "${profile}"

  say ""
  say "Setup complete."
  say "Start Tater with:"
  say ""
  say "  sh run_ui.sh"
  say ""
}

main "$@"
