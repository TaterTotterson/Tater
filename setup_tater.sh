#!/bin/sh
set -eu

VENV_DIR=".venv"
RUNTIME_DIR=".runtime"
PROFILE_FILE="${RUNTIME_DIR}/setup_profile"
PROFILE_ENV="${RUNTIME_DIR}/tater_profile.env"
REQUIREMENTS_FILE="requirements.txt"

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
  for candidate in python3.11 python3 python; do
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

ensure_venv() {
  profile="$1"
  python_bin="$2"
  existing_profile=""
  if [ -f "${PROFILE_FILE}" ]; then
    existing_profile="$(cat "${PROFILE_FILE}" 2>/dev/null || true)"
  fi

  if [ -d "${VENV_DIR}" ] && [ "${existing_profile}" != "${profile}" ]; then
    warn "Existing ${VENV_DIR} was prepared for '${existing_profile:-unknown}', not '${profile}'."
    if confirm "Rebuild ${VENV_DIR} for ${profile}? [y/N]" "n"; then
      rm -rf "${VENV_DIR}"
    else
      fail "Setup cancelled. Re-run with the matching profile or rebuild the venv."
    fi
  fi

  if [ ! -x "${VENV_DIR}/bin/python" ]; then
    info "Creating ${VENV_DIR}"
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

install_base() {
  venv_python="$1"
  info "Upgrading pip tooling"
  "${venv_python}" -m pip install --upgrade pip setuptools wheel
}

install_cpu() {
  venv_python="$1"
  info "Installing Tater dependencies"
  "${venv_python}" -m pip install -r "${REQUIREMENTS_FILE}"
}

install_macos() {
  venv_python="$1"
  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Darwin" ]; then
    warn "macOS profile selected on a non-macOS host."
  else
    arch="$(uname -m 2>/dev/null || printf unknown)"
    if [ "${arch}" = "arm64" ]; then
      ok "Detected Apple Silicon (${arch})"
    else
      warn "Detected macOS ${arch}. This profile is tuned for Apple Silicon but may still run CPU-first."
    fi
    if ! command -v brew >/dev/null 2>&1; then
      warn "Homebrew was not found. If installs fail, install Homebrew packages: ffmpeg libolm pkg-config."
    else
      warn "If native package builds fail, run: brew install ffmpeg libolm pkg-config"
    fi
  fi
  info "Installing Tater dependencies for macOS"
  "${venv_python}" -m pip install -r "${REQUIREMENTS_FILE}"
  info "Installing Apple-native speech extras"
  if ! "${venv_python}" -m pip install mlx-whisper kokoro; then
    warn "Apple-native speech extras failed to install. Tater will still run with Faster Whisper/Kokoro CPU fallbacks."
  fi
}

install_nvidia() {
  venv_python="$1"
  info "Installing NVIDIA PyTorch CUDA wheels"
  "${venv_python}" -m pip install --index-url https://download.pytorch.org/whl/cu128 torch torchaudio
  info "Installing CUDA runtime Python packages"
  "${venv_python}" -m pip install "nvidia-cublas-cu12" "nvidia-cudnn-cu12==9.*"
  info "Installing Tater dependencies"
  "${venv_python}" -m pip install -r "${REQUIREMENTS_FILE}"
  info "Switching ONNX Runtime to GPU build"
  "${venv_python}" -m pip uninstall -y onnxruntime >/dev/null 2>&1 || true
  "${venv_python}" -m pip install onnxruntime-gpu
}

install_rocm() {
  venv_python="$1"
  rocm_index="${TATER_ROCM_PYTORCH_INDEX_URL:-https://download.pytorch.org/whl/rocm6.4}"
  warn "AMD ROCm support is Linux-only and depends on the ROCm runtime installed for your GPU/APU."
  warn "Strix Halo systems may need newer AMD ROCm wheels; override with TATER_ROCM_PYTORCH_INDEX_URL if needed."
  info "Installing AMD ROCm PyTorch wheels from ${rocm_index}"
  "${venv_python}" -m pip install --index-url "${rocm_index}" torch torchaudio
  info "Installing Tater dependencies"
  "${venv_python}" -m pip install -r "${REQUIREMENTS_FILE}"
  info "Installing PyTorch Kokoro runtime"
  if ! "${venv_python}" -m pip install kokoro; then
    warn "Kokoro PyTorch failed to install. Tater will still run with CPU/ONNX TTS fallbacks."
  fi
}

install_jetson_like() {
  venv_python="$1"
  profile="$2"
  tmp_req="$(mktemp "${TMPDIR:-/tmp}/tater-requirements.XXXXXX")"
  trap 'rm -f "${tmp_req}"' EXIT

  filtered_requirements "${tmp_req}"
  info "Installing Tater dependencies without replacing JetPack PyTorch"
  "${venv_python}" -m pip install -r "${tmp_req}"

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

  {
    say "# Generated by setup_tater.sh"
    say "export TATER_SETUP_PROFILE=\"\${TATER_SETUP_PROFILE:-${profile}}\""
    say "export TATER_SPEECH_ACCELERATION=\"\${TATER_SPEECH_ACCELERATION:-${speech_acceleration}}\""
    say "export TATER_FASTER_WHISPER_COMPUTE_TYPE=\"\${TATER_FASTER_WHISPER_COMPUTE_TYPE:-${compute_type}}\""
    say "export TATER_KOKORO_ENGINE=\"\${TATER_KOKORO_ENGINE:-auto}\""
    if [ "${torch_mps_fallback}" ]; then
      say "export PYTORCH_ENABLE_MPS_FALLBACK=\"\${PYTORCH_ENABLE_MPS_FALLBACK:-${torch_mps_fallback}}\""
    fi
  } > "${PROFILE_ENV}"

  printf '%s\n' "${profile}" > "${PROFILE_FILE}"
}

verify_install() {
  venv_python="$1"
  profile="$2"
  info "Checking installed runtime"
  "${venv_python}" - <<'PY'
import importlib.util

required = ["fastapi", "uvicorn", "redis", "aioesphomeapi"]
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
