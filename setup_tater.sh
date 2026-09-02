#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd "$(dirname "$0")" && pwd -P)"
cd "${SCRIPT_DIR}"

VENV_DIR="${TATER_VENV_DIR:-.venv}"
RUNTIME_DIR="${TATER_RUNTIME_DIR:-.runtime}"
AGENT_ROOT="${TATER_AGENT_ROOT:-agent_lab}"
PROFILE_FILE="${TATER_SETUP_PROFILE_FILE:-${RUNTIME_DIR}/setup_profile}"
PROFILE_ENV="${TATER_PROFILE_ENV:-${RUNTIME_DIR}/tater_profile.env}"
REQUIREMENTS_FILE="${TATER_REQUIREMENTS_FILE:-requirements.txt}"
EDGE_REQUIREMENTS_FILE="${TATER_EDGE_REQUIREMENTS_FILE:-requirements-edge.txt}"
LLAMA_CPP_REPO="${TATER_LLAMA_CPP_REPO:-https://github.com/ggml-org/llama.cpp.git}"
LLAMA_CPP_REF="${TATER_LLAMA_CPP_REF:-master}"
LLAMA_CPP_DIR="${TATER_LLAMA_CPP_DIR:-${RUNTIME_DIR}/llama.cpp}"
LLAMA_CPP_SERVER_BIN="${TATER_LLAMA_CPP_SERVER_BIN:-${LLAMA_CPP_DIR}/build/bin/llama-server}"
ROCM_LIBXML2_COMPAT_DIR="${TATER_ROCM_LIBXML2_COMPAT_DIR:-${RUNTIME_DIR}/rocm-libxml2-compat}"
ROCM_LIBXML2_COMPAT_URL="https://launchpad.net/ubuntu/+archive/primary/+files/libxml2_2.12.7+dfsg+really2.9.14-0.4ubuntu0.4_amd64.deb"
ROCM_LIBXML2_COMPAT_SHA256="685e94ff7fd7ad869894c2317ab9473075536a5c74c092ca5a9cd5876acaaf6c"
MANAGED_PYTHON_RELEASE="20260825"
MANAGED_PYTHON_311_VERSION="3.11.16"
MANAGED_PYTHON_311_X86_64_SHA256="25844eb97cdc72cdc78addaad0969ce3b2133a4de54bfcfa4d57f8a6d095eaab"
MANAGED_PYTHON_311_AARCH64_SHA256="93fd0d922d88b758a8df277bf12b601fe7c35543a2feceaf10df8496758d28ea"
MANAGED_PYTHON_312_VERSION="3.12.14"
MANAGED_PYTHON_312_X86_64_SHA256="cbdd2f0cf02f941bc5c81e546f377275e322733abffe805ac29d2b7e8a58f7e3"
MANAGED_PYTHON_312_AARCH64_SHA256="70162d3fa61a7bf52a9f098ad6f46046f9813ab50e0d2b3cfeb81ee1bad78f1c"
AMD_RYZEN_AI_ROCM_VERSION="10.0.0"
AMD_RYZEN_AI_PYTORCH_INDEX_URL="https://stable.repo.amd.com/rocm/whl-next/"
AMD_RYZEN_AI_TORCH_VERSION="2.13.0+rocm10.0.0"
AMD_RYZEN_AI_TORCHVISION_VERSION="0.28.0+rocm10.0.0"
AMD_RYZEN_AI_TORCHAUDIO_VERSION="2.11.0.2+rocm10.0.0"
AIRPLAY_CLI_VERSION="0.4.12"
SHAIRPORT_SYNC_VERSION="5.2.1"
AIRPLAY_SENDER_BIN=""
AIRPLAY_RECEIVER_BIN=""
AIRPLAY_FFMPEG_BIN=""

# GPU wheels can be several gigabytes. Avoid keeping a second copy in pip's
# shared download cache during setup.
PIP_NO_CACHE_DIR="1"
export PIP_NO_CACHE_DIR

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
  say "Usage: sh setup_tater.sh [cpu|macos|nvidia|rocm|jetson|thor|edge]"
  say ""
  say "Profiles:"
  say "  cpu     Local CPU-first install for most systems."
  say "  macos   Native Apple Silicon/macOS install with MLX and MPS where supported."
  say "  nvidia  Native desktop/server NVIDIA install for amd64 CUDA PCs."
  say "  rocm    Native AMD ROCm install for Radeon / Strix Halo systems."
  say "  jetson  Native Jetson install that uses JetPack/system AI packages."
  say "  thor    Native Jetson Thor install for JetPack 7 / CUDA 13 systems."
  say "  edge    Remote-only install for Pi-class and memory-constrained hosts."
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
  say "  7) Edge / remote-only"
  say "     Full Tater app without local model runtimes; intended for Pi-class hosts."
  say ""
  printf "Select profile [1-7]: "
  read -r choice

  case "${choice}" in
    1) SELECTED_PROFILE="cpu" ;;
    2) SELECTED_PROFILE="macos" ;;
    3) SELECTED_PROFILE="nvidia" ;;
    4) SELECTED_PROFILE="rocm" ;;
    5) SELECTED_PROFILE="jetson" ;;
    6) SELECTED_PROFILE="thor" ;;
    7) SELECTED_PROFILE="edge" ;;
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
    edge|edge-remote|edge_remote|remote|remote-only|remote_only|sat1|sat1-edge|sat1_edge) printf '%s' "edge" ;;
    -h|--help|help) usage; exit 0 ;;
    *) fail "Unknown setup profile: $1" ;;
  esac
}

find_python() {
  preferred_minor="${1:-}"
  if [ "${PYTHON:-}" ] && command -v "${PYTHON}" >/dev/null 2>&1; then
    printf '%s' "${PYTHON}"
    return
  fi
  if [ "${preferred_minor}" ]; then
    for candidate in "python${preferred_minor}" "/usr/local/bin/python${preferred_minor}"; do
      if command -v "${candidate}" >/dev/null 2>&1; then
        printf '%s' "${candidate}"
        return
      fi
    done
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

python_version_supported() {
  case "$1" in
    3.11|3.12|3.13) return 0 ;;
    *) return 1 ;;
  esac
}

configure_managed_python() {
  requested_minor="$1"
  case "${requested_minor}" in
    3.11)
      MANAGED_PYTHON_VERSION="${MANAGED_PYTHON_311_VERSION}"
      MANAGED_PYTHON_X86_64_SHA256="${MANAGED_PYTHON_311_X86_64_SHA256}"
      MANAGED_PYTHON_AARCH64_SHA256="${MANAGED_PYTHON_311_AARCH64_SHA256}"
      ;;
    3.12)
      MANAGED_PYTHON_VERSION="${MANAGED_PYTHON_312_VERSION}"
      MANAGED_PYTHON_X86_64_SHA256="${MANAGED_PYTHON_312_X86_64_SHA256}"
      MANAGED_PYTHON_AARCH64_SHA256="${MANAGED_PYTHON_312_AARCH64_SHA256}"
      ;;
    *) fail "Automatic Python installation is not configured for Python ${requested_minor}." ;;
  esac
}

managed_python_dir() {
  printf '%s' "${TATER_MANAGED_PYTHON_DIR:-${RUNTIME_DIR}/python/cpython-${MANAGED_PYTHON_VERSION}+${MANAGED_PYTHON_RELEASE}}"
}

managed_python_asset() {
  if [ -z "${MANAGED_PYTHON_VERSION:-}" ]; then
    configure_managed_python 3.11
  fi
  case "$(uname -m 2>/dev/null || printf unknown)" in
    x86_64|amd64)
      MANAGED_PYTHON_ARCH="x86_64"
      MANAGED_PYTHON_SHA256="${MANAGED_PYTHON_X86_64_SHA256}"
      ;;
    aarch64|arm64)
      MANAGED_PYTHON_ARCH="aarch64"
      MANAGED_PYTHON_SHA256="${MANAGED_PYTHON_AARCH64_SHA256}"
      ;;
    *)
      fail "Automatic Python installation is not available for architecture $(uname -m 2>/dev/null || printf unknown). Install Python 3.11, 3.12, or 3.13, then rerun setup."
      ;;
  esac
  MANAGED_PYTHON_URL="https://github.com/astral-sh/python-build-standalone/releases/download/${MANAGED_PYTHON_RELEASE}/cpython-${MANAGED_PYTHON_VERSION}%2B${MANAGED_PYTHON_RELEASE}-${MANAGED_PYTHON_ARCH}-unknown-linux-gnu-install_only.tar.gz"
}

setup_file_sha256() {
  file="$1"
  fallback_python="${2:-}"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${file}" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${file}" | awk '{print $1}'
  elif [ "${fallback_python}" ]; then
    "${fallback_python}" -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "${file}"
  else
    fail "A SHA-256 tool is required to verify setup downloads."
  fi
}

cleanup_managed_python_stage() {
  [ -z "${managed_python_archive:-}" ] || rm -f "${managed_python_archive}"
  [ -z "${managed_python_stage:-}" ] || rm -rf "${managed_python_stage}"
}

install_managed_python() {
  bootstrap_python="$1"
  requested_minor="${2:-3.11}"
  configure_managed_python "${requested_minor}"
  managed_root="$(managed_python_dir)"
  managed_bin="${managed_root}/bin/python${requested_minor}"

  if [ -x "${managed_bin}" ] && [ "$(python_version "${managed_bin}" 2>/dev/null || true)" = "${requested_minor}" ]; then
    MANAGED_PYTHON_BIN="${managed_bin}"
    ok "Using managed Python ${MANAGED_PYTHON_VERSION}"
    return
  fi

  if ! truthy_env "${TATER_SETUP_INSTALL_MANAGED_PYTHON:-1}"; then
    fail "Tater requires Python 3.11, 3.12, or 3.13. Install one of those versions, or enable TATER_SETUP_INSTALL_MANAGED_PYTHON."
  fi
  [ "$(uname -s 2>/dev/null || printf unknown)" = "Linux" ] || fail "Tater requires Python 3.11, 3.12, or 3.13. Install a supported version, then rerun setup."
  command -v tar >/dev/null 2>&1 || fail "Automatic Python installation requires tar."

  managed_python_asset
  managed_parent="$(dirname "${managed_root}")"
  mkdir -p "${managed_parent}"
  managed_python_archive="$(mktemp "${managed_parent}/.cpython-download.XXXXXX")"
  managed_python_stage="$(mktemp -d "${managed_parent}/.cpython-extract.XXXXXX")"
  trap cleanup_managed_python_stage EXIT

  info "Installing a private Python ${MANAGED_PYTHON_VERSION} runtime for Tater"
  download_setup_file "${MANAGED_PYTHON_URL}" "${managed_python_archive}" "${bootstrap_python}"
  actual_sha256="$(setup_file_sha256 "${managed_python_archive}" "${bootstrap_python}")"
  if [ "${actual_sha256}" != "${MANAGED_PYTHON_SHA256}" ]; then
    fail "The downloaded Python runtime did not match its expected SHA-256 checksum."
  fi
  tar -xzf "${managed_python_archive}" -C "${managed_python_stage}"
  extracted_root="${managed_python_stage}/python"
  extracted_bin="${extracted_root}/bin/python${requested_minor}"
  if [ ! -x "${extracted_bin}" ] || [ "$(python_version "${extracted_bin}" 2>/dev/null || true)" != "${requested_minor}" ]; then
    fail "The downloaded Python runtime did not contain a working Python ${requested_minor} interpreter."
  fi

  if [ -e "${managed_root}" ]; then
    warn "Replacing an incomplete managed Python runtime"
    rm -rf "${managed_root}"
  fi
  mv "${extracted_root}" "${managed_root}"
  cleanup_managed_python_stage
  managed_python_archive=""
  managed_python_stage=""
  trap - EXIT

  MANAGED_PYTHON_BIN="${managed_bin}"
  ok "Managed Python ${MANAGED_PYTHON_VERSION} is ready"
}

select_supported_python() {
  candidate="$1"
  preferred_minor="${2:-}"
  version="$(python_version "${candidate}")"
  if python_version_supported "${version}" && { [ -z "${preferred_minor}" ] || [ "${version}" = "${preferred_minor}" ]; }; then
    SUPPORTED_PYTHON_BIN="${candidate}"
    ok "Using ${candidate} ${version}"
    return
  fi

  if [ "${PYTHON:-}" ]; then
    if [ "${preferred_minor}" ] && [ "${version}" != "${preferred_minor}" ]; then
      fail "This hardware profile requires Python ${preferred_minor}, but PYTHON points to ${version}. Set PYTHON to Python ${preferred_minor}, or unset it so setup can install a private runtime."
    fi
    fail "PYTHON points to unsupported Python ${version}. Set PYTHON to Python 3.11, 3.12, or 3.13, or unset it so setup can install a private supported runtime."
  fi
  managed_minor="${preferred_minor:-3.11}"
  if python_version_supported "${version}"; then
    warn "The selected hardware profile requires Python ${managed_minor}; using a private runtime"
  else
    warn "Python ${version} is not supported by Tater's AI dependencies; using a private Python ${managed_minor} runtime"
  fi
  install_managed_python "${candidate}" "${managed_minor}"
  SUPPORTED_PYTHON_BIN="${MANAGED_PYTHON_BIN}"
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

is_strix_halo_host() {
  override="$(printf '%s' "${TATER_SETUP_STRIX_HALO:-}" | tr '[:upper:]' '[:lower:]')"
  case "${override}" in
    1|true|yes|y|on) return 0 ;;
    0|false|no|n|off) return 1 ;;
  esac
  [ "$(uname -s 2>/dev/null || printf unknown)" = "Linux" ] || return 1
  [ -r /proc/cpuinfo ] || return 1
  grep -Eiq 'AMD[[:space:]]+RYZEN[[:space:]]+AI[[:space:]]+MAX' /proc/cpuinfo
}

is_amd_ryzen_ai_host() {
  override="$(printf '%s' "${TATER_SETUP_RYZEN_AI:-}" | tr '[:upper:]' '[:lower:]')"
  case "${override}" in
    1|true|yes|y|on) return 0 ;;
    0|false|no|n|off) return 1 ;;
  esac
  [ "$(uname -s 2>/dev/null || printf unknown)" = "Linux" ] || return 1
  [ -r /proc/cpuinfo ] || return 1
  grep -Eiq 'AMD.*RYZEN.*AI' /proc/cpuinfo
}

warn_if_unvalidated_ryzen_ai_os() {
  [ -r /etc/os-release ] || return
  os_id="$(. /etc/os-release; printf '%s' "${ID:-}")"
  os_version="$(. /etc/os-release; printf '%s' "${VERSION_ID:-}")"
  if [ "${os_id}" = "ubuntu" ] && [ "${os_version}" != "24.04" ] && [ "${os_version}" != "26.04" ]; then
    warn "AMD validates the Ryzen AI ROCm ${AMD_RYZEN_AI_ROCM_VERSION} package set on Ubuntu 24.04.4 and 26.04; detected Ubuntu ${os_version}. Setup will continue and verify GPU access before reporting success."
  fi
}

existing_rocm_environment_ready() {
  if truthy_env "${TATER_SETUP_UPGRADE_ROCM:-}"; then
    return 1
  fi
  [ -x "${VENV_DIR}/bin/python" ] || return 1
  [ "$(cat "${PROFILE_FILE}" 2>/dev/null || true)" = "rocm" ] || return 1
  rocm_torch_ready "${VENV_DIR}/bin/python"
}

rocm_torch_ready() {
  "$1" -c 'import torch; raise SystemExit(0 if torch.version.hip and torch.cuda.is_available() else 1)' >/dev/null 2>&1
}

amd_rocm_gfx_target_from_tools() {
  detected_target=""
  if command -v rocm_agent_enumerator >/dev/null 2>&1; then
    detected_target="$(rocm_agent_enumerator 2>/dev/null | awk '/^gfx[0-9a-f]+$/ && $0 != "gfx000" { print; exit }')"
  fi
  if [ -z "${detected_target}" ] && command -v rocminfo >/dev/null 2>&1; then
    detected_target="$(rocminfo 2>/dev/null | awk '/^[[:space:]]*Name:[[:space:]]+gfx[0-9a-f]+/ { print $2; exit }')"
  fi
  printf '%s' "${detected_target}"
}

validated_rocm_gfx_target() {
  requested_target="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  requested_target="${requested_target#device-}"
  case "${requested_target}" in
    all|gfx908|gfx90a|gfx942|gfx950|gfx1030|gfx1100|gfx1101|gfx1102|gfx1103|gfx1150|gfx1151|gfx1152|gfx1153|gfx1200|gfx1201)
      printf '%s' "${requested_target}"
      ;;
    *)
      fail "Unsupported TATER_ROCM_GFX_TARGET value: ${requested_target}. Use a ROCm 10 gfx target such as gfx1150 or gfx1151."
      ;;
  esac
}

amd_ryzen_ai_gfx_target() {
  if [ "${TATER_ROCM_GFX_TARGET:-}" ]; then
    validated_rocm_gfx_target "${TATER_ROCM_GFX_TARGET}"
    return
  fi

  detected_target="$(amd_rocm_gfx_target_from_tools)"
  if [ "${detected_target}" ]; then
    validated_rocm_gfx_target "${detected_target}"
    return
  fi

  if is_strix_halo_host; then
    printf '%s' "gfx1151"
    return
  fi
  if [ -r /proc/cpuinfo ] && grep -Eiq 'AMD.*RYZEN.*AI.*(HX[[:space:]]*(370|375|470|475)|[[:space:]](365|465)([^0-9]|$))' /proc/cpuinfo; then
    printf '%s' "gfx1150"
    return
  fi

  warn "Could not determine the Ryzen AI GPU target; installing AMD's all-device ROCm package set" >&2
  printf '%s' "all"
}

install_amd_ryzen_ai_pytorch() {
  venv_python="$1"
  gfx_target="$(amd_ryzen_ai_gfx_target)"
  device_extra="device-${gfx_target}"
  info "Installing AMD PyTorch ${AMD_RYZEN_AI_TORCH_VERSION} for ROCm ${AMD_RYZEN_AI_ROCM_VERSION} (${gfx_target})"
  "${venv_python}" -m pip install --upgrade \
    --index-url "${AMD_RYZEN_AI_PYTORCH_INDEX_URL}" \
    "torch[${device_extra}]==${AMD_RYZEN_AI_TORCH_VERSION}" \
    "torchvision[${device_extra}]==${AMD_RYZEN_AI_TORCHVISION_VERSION}" \
    "torchaudio==${AMD_RYZEN_AI_TORCHAUDIO_VERSION}"
}

user_is_in_group() {
  setup_user="$1"
  required_group="$2"
  user_groups="$(id -Gn "${setup_user}" 2>/dev/null || true)"
  case " ${user_groups} " in
    *" ${required_group} "*) return 0 ;;
    *) return 1 ;;
  esac
}

ensure_amd_gpu_group_membership() {
  setup_user="${TATER_SETUP_USER:-${SUDO_USER:-$(id -un)}}"
  missing_groups=""
  for required_group in render video; do
    if ! user_is_in_group "${setup_user}" "${required_group}"; then
      missing_groups="${missing_groups}${missing_groups:+,}${required_group}"
    fi
  done

  if [ -z "${missing_groups}" ]; then
    fail "${setup_user} belongs to the render and video groups, but this login session does not have GPU access yet. Reboot, then rerun setup."
  fi
  if ! truthy_env "${TATER_SETUP_INSTALL_SYSTEM_DEPS:-1}"; then
    fail "${setup_user} must be added to the render and video groups for AMD GPU access. Run 'sudo usermod -a -G render,video ${setup_user}', reboot, then rerun setup."
  fi

  info "Adding ${setup_user} to the render and video groups for AMD GPU access"
  run_privileged usermod -a -G render,video "${setup_user}"
  fail "Added ${setup_user} to the render and video groups. Reboot this machine, then rerun setup."
}

ensure_amd_gpu_device_access() {
  kfd_path="${TATER_AMD_KFD_PATH:-/dev/kfd}"
  [ "$(uname -s 2>/dev/null || printf unknown)" = "Linux" ] || fail "The AMD ROCm profile requires Linux."
  if [ ! -e "${kfd_path}" ]; then
    fail "The AMD compute device ${kfd_path} is unavailable. Install a kernel/AMDGPU stack that supports this processor and reboot before rerunning setup."
  fi
  if [ ! -r "${kfd_path}" ] || [ ! -w "${kfd_path}" ]; then
    ensure_amd_gpu_group_membership
  fi
  ok "AMD GPU device access is ready"
}

missing_linux_build_tools() {
  missing=""
  for tool in git cmake make cc c++; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
      missing="${missing} ${tool}"
    fi
  done
  printf '%s' "${missing# }"
}

run_privileged() {
  if [ "$(id -u)" = "0" ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  elif command -v doas >/dev/null 2>&1; then
    doas "$@"
  else
    fail "System dependencies are missing and neither sudo nor doas is available."
  fi
}

check_shairport_sync_receiver() {
  receiver_bin="$1"
  [ -x "${receiver_bin}" ] \
    && "${receiver_bin}" -V 2>&1 | grep -q "^${SHAIRPORT_SYNC_VERSION}-" \
    && "${receiver_bin}" -h 2>&1 | grep -q -- "--service-type" \
    && "${receiver_bin}" -h 2>&1 | grep -q -- "stdout"
}

install_linux_airplay_build_dependencies() {
  if ! truthy_env "${TATER_SETUP_INSTALL_SYSTEM_DEPS:-1}"; then
    fail "The pinned AirPlay receiver is missing. Install its build dependencies or enable TATER_SETUP_INSTALL_SYSTEM_DEPS."
  fi

  info "Installing native AirPlay receiver dependencies"
  if command -v apt-get >/dev/null 2>&1; then
    run_privileged apt-get update
    run_privileged apt-get install -y \
      autoconf automake libtool pkg-config patch curl ca-certificates build-essential \
      libssl-dev libconfig-dev libpopt-dev libsoxr-dev ffmpeg
  elif command -v dnf >/dev/null 2>&1; then
    run_privileged dnf install -y \
      autoconf automake libtool pkgconf-pkg-config patch curl gcc gcc-c++ make \
      openssl-devel libconfig-devel popt-devel soxr-devel ffmpeg
  elif command -v yum >/dev/null 2>&1; then
    run_privileged yum install -y \
      autoconf automake libtool pkgconfig patch curl gcc gcc-c++ make \
      openssl-devel libconfig-devel popt-devel soxr-devel ffmpeg
  elif command -v pacman >/dev/null 2>&1; then
    run_privileged pacman -S --needed --noconfirm \
      base-devel autoconf automake libtool pkgconf patch curl openssl libconfig popt libsoxr ffmpeg
  elif command -v zypper >/dev/null 2>&1; then
    run_privileged zypper --non-interactive install -y \
      autoconf automake libtool pkg-config patch curl gcc gcc-c++ make \
      libopenssl-devel libconfig-devel popt-devel libsoxr-devel ffmpeg
  elif command -v apk >/dev/null 2>&1; then
    run_privileged apk add \
      build-base autoconf automake libtool pkgconf patch curl openssl-dev \
      libconfig-dev popt-dev soxr-dev ffmpeg
  elif command -v xbps-install >/dev/null 2>&1; then
    run_privileged xbps-install -Sy \
      base-devel autoconf automake libtool pkg-config patch curl openssl-devel \
      libconfig-devel popt-devel libsoxr-devel ffmpeg
  else
    fail "No supported package manager was found for the native AirPlay receiver dependencies."
  fi
}

install_linux_airplay_capability_tools() {
  if command -v getcap >/dev/null 2>&1 && command -v setcap >/dev/null 2>&1; then
    return
  fi
  if ! truthy_env "${TATER_SETUP_INSTALL_SYSTEM_DEPS:-1}"; then
    fail "The AirPlay sender needs Linux capability tools. Install them or enable TATER_SETUP_INSTALL_SYSTEM_DEPS."
  fi

  info "Installing Linux AirPlay permission tools"
  if command -v apt-get >/dev/null 2>&1; then
    run_privileged apt-get update
    run_privileged apt-get install -y libcap2-bin
  elif command -v dnf >/dev/null 2>&1; then
    run_privileged dnf install -y libcap
  elif command -v yum >/dev/null 2>&1; then
    run_privileged yum install -y libcap
  elif command -v pacman >/dev/null 2>&1; then
    run_privileged pacman -S --needed --noconfirm libcap
  elif command -v zypper >/dev/null 2>&1; then
    run_privileged zypper --non-interactive install -y libcap-progs
  elif command -v apk >/dev/null 2>&1; then
    run_privileged apk add libcap-utils
  elif command -v xbps-install >/dev/null 2>&1; then
    run_privileged xbps-install -Sy libcap-progs
  else
    fail "No supported package manager was found for the Linux AirPlay permission tools."
  fi
}

linux_airplay_ptp_ports_available() {
  venv_python="$1"
  "${venv_python}" - <<'PY'
import socket

sockets = []
try:
    for port in (319, 320):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", port))
        sockets.append(sock)
except OSError:
    raise SystemExit(1)
finally:
    for sock in sockets:
        sock.close()
PY
}

ensure_linux_airplay_sender_permissions() {
  sender_bin="$1"
  venv_python="$2"
  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Linux" ]; then
    return
  fi
  if linux_airplay_ptp_ports_available "${venv_python}"; then
    ok "AirPlay PTP ports are available"
    return
  fi
  if command -v getcap >/dev/null 2>&1 \
    && getcap "${sender_bin}" 2>/dev/null | grep -q "cap_net_bind_service"; then
    ok "AirPlay sender has permission to use its PTP ports"
    return
  fi

  install_linux_airplay_capability_tools
  run_privileged setcap cap_net_bind_service=+ep "${sender_bin}"
  getcap "${sender_bin}" 2>/dev/null | grep -q "cap_net_bind_service" \
    || fail "Could not grant the AirPlay sender permission to use UDP ports 319 and 320."
  ok "AirPlay sender has permission to use its PTP ports"
}

resolve_airplay_ffmpeg() {
  venv_python="$1"
  configured_ffmpeg="${TATER_FFMPEG_PATH:-${FFMPEG_PATH:-}}"
  if [ "${configured_ffmpeg}" ] && [ -x "${configured_ffmpeg}" ]; then
    printf '%s' "${configured_ffmpeg}"
    return
  fi
  if command -v ffmpeg >/dev/null 2>&1; then
    command -v ffmpeg
    return
  fi
  "${venv_python}" - <<'PY'
import os
import imageio_ffmpeg

path = imageio_ffmpeg.get_ffmpeg_exe()
if not os.path.isfile(path) or not os.access(path, os.X_OK):
    raise SystemExit("imageio-ffmpeg did not provide an executable")
print(path)
PY
}

install_airplay_runtime_dependencies() {
  profile="$1"
  venv_python="$2"
  platform_name="$(uname -s 2>/dev/null || printf unknown)"
  runtime_receiver="${RUNTIME_DIR}/external_audio/shairport-sync-v${SHAIRPORT_SYNC_VERSION}/bin/shairport-sync"
  configured_receiver="${TATER_SHAIRPORT_SYNC_PATH:-}"
  if [ "${configured_receiver}" ] && check_shairport_sync_receiver "${configured_receiver}"; then
    AIRPLAY_RECEIVER_BIN="${configured_receiver}"
  elif check_shairport_sync_receiver "${runtime_receiver}"; then
    AIRPLAY_RECEIVER_BIN="${runtime_receiver}"
  elif [ "${platform_name}" = "Darwin" ]; then
    info "Installing the pinned AirPlay receiver for macOS"
    TATER_RUNTIME_DIR="${RUNTIME_DIR}" \
      sh "${SCRIPT_DIR}/scripts/install_shairport_sync_receiver_macos.sh"
    AIRPLAY_RECEIVER_BIN="${runtime_receiver}"
  elif [ "${platform_name}" = "Linux" ]; then
    install_linux_airplay_build_dependencies
    TATER_RUNTIME_DIR="${RUNTIME_DIR}" \
      sh "${SCRIPT_DIR}/scripts/install_shairport_sync_receiver_linux.sh"
    AIRPLAY_RECEIVER_BIN="${runtime_receiver}"
  else
    fail "Native AirPlay receiver setup is not supported on ${platform_name}."
  fi
  check_shairport_sync_receiver "${AIRPLAY_RECEIVER_BIN}" \
    || fail "The AirPlay receiver did not pass its version and feature checks."
  ok "AirPlay receiver ${SHAIRPORT_SYNC_VERSION} is ready"

  AIRPLAY_FFMPEG_BIN="$(resolve_airplay_ffmpeg "${venv_python}")"
  [ -x "${AIRPLAY_FFMPEG_BIN}" ] || fail "FFmpeg is required for AirPlay playback."
  "${AIRPLAY_FFMPEG_BIN}" -version >/dev/null 2>&1 \
    || fail "The selected FFmpeg executable did not pass its self-check."
  ok "AirPlay FFmpeg runtime is ready"

  AIRPLAY_SENDER_BIN="$(
    TATER_RUNTIME_DIR="${RUNTIME_DIR}" \
    TATER_AIRPLAY_CLI_PATH="${TATER_AIRPLAY_CLI_PATH:-}" \
      "${venv_python}" -c 'from airplay_bridge import ensure_airplay_cli; print(ensure_airplay_cli())'
  )"
  [ -x "${AIRPLAY_SENDER_BIN}" ] || fail "The AirPlay sender was not installed."
  "${AIRPLAY_SENDER_BIN}" --check >/dev/null 2>&1 \
    || fail "The AirPlay sender did not pass its self-check."
  ok "AirPlay sender ${AIRPLAY_CLI_VERSION} is ready"
  ensure_linux_airplay_sender_permissions "${AIRPLAY_SENDER_BIN}" "${venv_python}"
}

ensure_linux_build_tools() {
  profile="$1"
  if [ "${profile}" = "macos" ] || [ "${profile}" = "edge" ] || [ "$(uname -s 2>/dev/null || printf unknown)" != "Linux" ]; then
    return
  fi
  if [ "${TATER_SETUP_LLAMA_CPP_NATIVE:-1}" = "0" ]; then
    return
  fi

  missing="$(missing_linux_build_tools)"
  if [ -z "${missing}" ]; then
    ok "Linux build tools are ready"
  else
    if ! truthy_env "${TATER_SETUP_INSTALL_SYSTEM_DEPS:-1}"; then
      fail "Missing Linux build tools: ${missing}. Install them or enable TATER_SETUP_INSTALL_SYSTEM_DEPS."
    fi

    info "Installing required Linux build tools: ${missing}"
    if command -v apt-get >/dev/null 2>&1; then
      run_privileged apt-get update
      run_privileged apt-get install -y git cmake build-essential
    elif command -v dnf >/dev/null 2>&1; then
      run_privileged dnf install -y git cmake gcc gcc-c++ make
    elif command -v yum >/dev/null 2>&1; then
      run_privileged yum install -y git cmake gcc gcc-c++ make
    elif command -v pacman >/dev/null 2>&1; then
      run_privileged pacman -S --needed --noconfirm git cmake base-devel
    elif command -v zypper >/dev/null 2>&1; then
      run_privileged zypper --non-interactive install -y git cmake gcc gcc-c++ make
    elif command -v apk >/dev/null 2>&1; then
      run_privileged apk add git cmake build-base
    elif command -v xbps-install >/dev/null 2>&1; then
      run_privileged xbps-install -Sy git cmake base-devel
    else
      fail "Missing Linux build tools: ${missing}. No supported package manager was found."
    fi

    missing="$(missing_linux_build_tools)"
    [ -z "${missing}" ] || fail "Linux build tools are still missing after installation: ${missing}"
    ok "Linux build tools are ready"
  fi

  if [ "${profile}" = "rocm" ] && [ -z "${TATER_LLAMA_CPP_CMAKE_ARGS:-}" ]; then
    prepare_amd_llama_cpp_backend
  fi
}

find_rocm_sdk_root() {
  candidates=""
  if [ "${TATER_ROCM_PATH:-}" ]; then
    candidates="${candidates} ${TATER_ROCM_PATH}"
  fi
  if [ "${ROCM_PATH:-}" ]; then
    candidates="${candidates} ${ROCM_PATH}"
  fi
  if command -v hipconfig >/dev/null 2>&1; then
    detected_rocm_root="$(hipconfig --path 2>/dev/null | head -n 1 || true)"
    [ -z "${detected_rocm_root}" ] || candidates="${candidates} ${detected_rocm_root}"
  fi
  candidates="${candidates} /opt/rocm"
  for candidate in ${candidates}; do
    if [ -x "${candidate}/bin/hipcc" ] || [ -f "${candidate}/lib/cmake/hip/hip-config.cmake" ]; then
      printf '%s' "${candidate}"
      return
    fi
  done
  for candidate in /opt/rocm-*; do
    if [ -x "${candidate}/bin/hipcc" ] || [ -f "${candidate}/lib/cmake/hip/hip-config.cmake" ]; then
      printf '%s' "${candidate}"
      return
    fi
  done
  return 0
}

vulkan_build_dependencies_ready() {
  command -v glslc >/dev/null 2>&1 || return 1
  [ -f /usr/include/vulkan/vulkan.h ] || return 1
  [ -f /usr/include/spirv/unified1/spirv.hpp ] || return 1
}

ensure_vulkan_build_dependencies() {
  if vulkan_build_dependencies_ready; then
    ok "Vulkan build tools are ready"
    return
  fi
  if ! truthy_env "${TATER_SETUP_INSTALL_SYSTEM_DEPS:-1}"; then
    fail "The ROCm SDK is absent and Vulkan build dependencies are missing. Install libvulkan-dev, glslc, and spirv-headers, or enable TATER_SETUP_INSTALL_SYSTEM_DEPS."
  fi

  info "Installing Vulkan build tools for the AMD local LLM runtime"
  if command -v apt-get >/dev/null 2>&1; then
    run_privileged apt-get update
    run_privileged apt-get install -y libvulkan-dev glslc spirv-headers
  elif command -v dnf >/dev/null 2>&1; then
    run_privileged dnf install -y vulkan-loader-devel glslc spirv-headers
  elif command -v yum >/dev/null 2>&1; then
    run_privileged yum install -y vulkan-loader-devel glslc spirv-headers
  elif command -v pacman >/dev/null 2>&1; then
    run_privileged pacman -S --needed --noconfirm vulkan-headers shaderc spirv-headers
  elif command -v zypper >/dev/null 2>&1; then
    run_privileged zypper --non-interactive install -y vulkan-devel glslc spirv-headers
  elif command -v apk >/dev/null 2>&1; then
    run_privileged apk add vulkan-loader-dev shaderc spirv-headers
  else
    fail "The ROCm SDK is absent. Install this distribution's Vulkan loader headers, glslc, and SPIR-V headers, then rerun setup."
  fi
  vulkan_build_dependencies_ready || fail "Vulkan build dependencies are still missing after installation."
  ok "Vulkan build tools are ready"
}

prepare_amd_llama_cpp_backend() {
  requested_backend="$(printf '%s' "${TATER_LLAMA_CPP_ROCM_BACKEND:-auto}" | tr '[:upper:]' '[:lower:]')"
  rocm_sdk_root="$(find_rocm_sdk_root)"
  case "${requested_backend}" in
    auto)
      if [ "${rocm_sdk_root}" ]; then
        selected_backend="hip"
      else
        selected_backend="vulkan"
      fi
      ;;
    hip|vulkan) selected_backend="${requested_backend}" ;;
    *) fail "Unknown TATER_LLAMA_CPP_ROCM_BACKEND value: ${requested_backend}. Use auto, hip, or vulkan." ;;
  esac

  if [ "${selected_backend}" = "hip" ]; then
    [ "${rocm_sdk_root}" ] || fail "The HIP llama.cpp backend was requested, but no ROCm SDK was found. Install the ROCm HIP SDK or use TATER_LLAMA_CPP_ROCM_BACKEND=vulkan."
    ROCM_PATH="${rocm_sdk_root}"
    HIP_PATH="${rocm_sdk_root}"
    PATH="${rocm_sdk_root}/bin:${PATH}"
    CMAKE_PREFIX_PATH="${rocm_sdk_root}${CMAKE_PREFIX_PATH:+;${CMAKE_PREFIX_PATH}}"
    export ROCM_PATH HIP_PATH PATH CMAKE_PREFIX_PATH
    ok "Using ROCm SDK at ${rocm_sdk_root} for llama.cpp"
  else
    if [ "${rocm_sdk_root}" ]; then
      info "Building llama.cpp with the requested Vulkan GPU backend"
    else
      warn "ROCm SDK not found; building llama.cpp with Vulkan GPU acceleration"
    fi
    ensure_vulkan_build_dependencies
  fi
  TATER_LLAMA_CPP_ROCM_BACKEND_SELECTED="${selected_backend}"
}

create_python_venv() {
  profile="$1"
  python_bin="$2"
  if [ "${profile}" = "jetson" ] || [ "${profile}" = "thor" ]; then
    "${python_bin}" -m venv --system-site-packages "${VENV_DIR}"
  else
    "${python_bin}" -m venv "${VENV_DIR}"
  fi
}

venv_failure_is_missing_support() {
  error_output="$1"
  case "${error_output}" in
    *ensurepip*|*"No module named venv"*|*python3-venv*|*python3.*-venv*) return 0 ;;
    *) return 1 ;;
  esac
}

install_linux_python_venv_support() {
  python_bin="$1"
  version="$(python_version "${python_bin}")"
  versioned_apt_package="python${version}-venv"

  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Linux" ]; then
    fail "${python_bin} cannot create virtual environments. Install Python with venv/ensurepip support, then rerun setup."
  fi
  if ! truthy_env "${TATER_SETUP_INSTALL_SYSTEM_DEPS:-1}"; then
    fail "${python_bin} cannot create virtual environments. Install ${versioned_apt_package} (Debian/Ubuntu) or the equivalent package for this system, or enable TATER_SETUP_INSTALL_SYSTEM_DEPS."
  fi

  info "Installing Python ${version} virtual-environment support"
  if command -v apt-get >/dev/null 2>&1; then
    run_privileged apt-get update
    if ! run_privileged apt-get install -y "${versioned_apt_package}"; then
      warn "${versioned_apt_package} was unavailable; trying python3-venv"
      run_privileged apt-get install -y python3-venv
    fi
  elif command -v dnf >/dev/null 2>&1; then
    if ! run_privileged dnf install -y "python${version}-pip"; then
      run_privileged dnf install -y python3-pip
    fi
  elif command -v yum >/dev/null 2>&1; then
    if ! run_privileged yum install -y "python${version}-pip"; then
      run_privileged yum install -y python3-pip
    fi
  elif command -v pacman >/dev/null 2>&1; then
    run_privileged pacman -S --needed --noconfirm python python-pip
  elif command -v zypper >/dev/null 2>&1; then
    run_privileged zypper --non-interactive install -y python3-pip
  elif command -v apk >/dev/null 2>&1; then
    run_privileged apk add python3 py3-pip py3-virtualenv
  elif command -v xbps-install >/dev/null 2>&1; then
    run_privileged xbps-install -Sy python3 python3-pip
  else
    fail "${python_bin} cannot create virtual environments and no supported package manager was found. Install venv/ensurepip support for Python ${version}, then rerun setup."
  fi
}

ensure_venv() {
  profile="$1"
  python_bin="$2"
  required_python_minor="${3:-}"
  existing_profile=""
  if [ -f "${PROFILE_FILE}" ]; then
    existing_profile="$(cat "${PROFILE_FILE}" 2>/dev/null || true)"
  fi

  if [ -f "${VENV_DIR}/pyvenv.cfg" ] && [ -x "${VENV_DIR}/bin/python" ] && ! "${VENV_DIR}/bin/python" -m pip --version >/dev/null 2>&1; then
    warn "Removing incomplete ${VENV_DIR} from a previous failed setup"
    rm -rf "${VENV_DIR}"
  fi

  if [ -x "${VENV_DIR}/bin/python" ]; then
    existing_venv_version="$(python_version "${VENV_DIR}/bin/python" 2>/dev/null || true)"
    if ! python_version_supported "${existing_venv_version}"; then
      warn "Removing ${VENV_DIR} created with unsupported Python ${existing_venv_version:-unknown}"
      rm -rf "${VENV_DIR}"
    elif [ -n "${required_python_minor}" ] && [ "${existing_venv_version}" != "${required_python_minor}" ]; then
      warn "Rebuilding ${VENV_DIR} with Python ${required_python_minor} required by ${profile}"
      rm -rf "${VENV_DIR}"
    fi
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
    venv_error=""
    if ! venv_error="$(create_python_venv "${profile}" "${python_bin}" 2>&1)"; then
      [ -z "${venv_error}" ] || printf '%s\n' "${venv_error}" >&2
      if ! venv_failure_is_missing_support "${venv_error}"; then
        fail "Failed to create ${VENV_DIR} with ${python_bin}."
      fi

      warn "Python $(python_version "${python_bin}") is missing virtual-environment support"
      install_linux_python_venv_support "${python_bin}"
      if [ -e "${VENV_DIR}" ]; then
        rm -rf "${VENV_DIR}"
      fi
      mkdir -p "$(dirname "${VENV_DIR}")"
      if ! create_python_venv "${profile}" "${python_bin}"; then
        fail "Failed to create ${VENV_DIR} after installing Python virtual-environment support."
      fi
    fi
  else
    ok "Using existing ${VENV_DIR}"
  fi

  if [ ! -x "${VENV_DIR}/bin/python" ] || ! "${VENV_DIR}/bin/python" -m pip --version >/dev/null 2>&1; then
    fail "${VENV_DIR} was created without a working pip installation. Install venv/ensurepip support for ${python_bin}, remove ${VENV_DIR}, and rerun setup."
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

install_macos_bundled_native_wheels() {
  venv_python="$1"
  wheel_dir="${SCRIPT_DIR}/vendor/wheels/macos"
  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Darwin" ]; then
    return
  fi
  if [ "$(uname -m 2>/dev/null || printf unknown)" != "arm64" ]; then
    return
  fi
  if [ ! -d "${wheel_dir}" ]; then
    return
  fi
  info "Installing bundled macOS native wheels"
  if "${venv_python}" -m pip install \
    --find-links "${wheel_dir}" \
    --only-binary python-olm \
    --only-binary redislite \
    "python-olm==3.2.16" \
    "redislite==6.2.912183"; then
    ok "Bundled macOS native wheels installed"
  else
    warn "Bundled macOS native wheels did not install. Setup will try the normal requirements next."
  fi
}

install_base() {
  venv_python="$1"
  info "Upgrading pip tooling"
  "${venv_python}" -m pip install --upgrade pip "setuptools<81" wheel
  cleanup_legacy_runtime "${venv_python}"
}

cleanup_legacy_runtime() {
  venv_python="$1"
  legacy_packages=""
  for package_name in \
    aioesphomeapi \
    nanowakeword \
    openwakeword \
    noiseprotocol \
    scikit-learn \
    narwhals \
    threadpoolctl \
    tzlocal \
    tzdata
  do
    if "${venv_python}" -m pip show "${package_name}" >/dev/null 2>&1; then
      legacy_packages="${legacy_packages} ${package_name}"
    fi
  done
  if [ -n "${legacy_packages}" ]; then
    info "Removing obsolete Tater Python dependencies"
    # Package names come only from the fixed allowlist above.
    # shellcheck disable=SC2086
    "${venv_python}" -m pip uninstall -y ${legacy_packages}
  fi

  remove_managed_venv_if_present "${RUNTIME_DIR}/models/face-id/venv" "obsolete Face ID worker environment"
  case "${RUNTIME_DIR}" in
    */runtime)
      support_root="$(dirname "${RUNTIME_DIR}")"
      remove_managed_venv_if_present "${support_root}/models/face-id/venv" "older Face ID worker environment"
      ;;
  esac

  legacy_firmware_root="${AGENT_ROOT}/esphome"
  firmware_root="${AGENT_ROOT}/firmware"
  if [ -d "${legacy_firmware_root}" ] && [ ! -e "${firmware_root}" ]; then
    info "Renaming the native firmware workspace to ${firmware_root}"
    mkdir -p "$(dirname "${firmware_root}")"
    mv "${legacy_firmware_root}" "${firmware_root}"
  fi
}

remove_managed_venv_if_present() {
  managed_venv="$1"
  label="$2"
  if [ ! -f "${managed_venv}/pyvenv.cfg" ]; then
    return
  fi
  info "Removing ${label}"
  rm -rf "${managed_venv}"
}

pip_install_requirements() {
  venv_python="$1"
  requirements_file="$2"
  # python-olm 3.2.16 bundles a pre-CMake-3.5 project and must build from
  # source on Python 3.13+. CMake 4 supports this external compatibility
  # floor without modifying the third-party source.
  CMAKE_POLICY_VERSION_MINIMUM="${CMAKE_POLICY_VERSION_MINIMUM:-3.5}" \
    "${venv_python}" -m pip install -r "${requirements_file}"
}

install_cpu() {
  venv_python="$1"
  tmp_req="$(mktemp "${TMPDIR:-/tmp}/tater-requirements-cpu.XXXXXX")"
  trap 'rm -f "${tmp_req}"' EXIT
  filtered_requirements "${tmp_req}"

  info "Installing CPU PyTorch wheels"
  "${venv_python}" -m pip install --index-url https://download.pytorch.org/whl/cpu torch torchaudio
  info "Installing Tater dependencies"
  pip_install_requirements "${venv_python}" "${tmp_req}"
  install_llama_cpp_native cpu
  rm -f "${tmp_req}"
  trap - EXIT
}

install_edge() {
  venv_python="$1"
  [ -f "${EDGE_REQUIREMENTS_FILE}" ] || fail "Missing edge requirements: ${EDGE_REQUIREMENTS_FILE}"
  info "Installing remote-only Tater dependencies"
  pip_install_requirements "${venv_python}" "${EDGE_REQUIREMENTS_FILE}"
}

check_llama_cpp_native() {
  server_bin="${1:-${LLAMA_CPP_SERVER_BIN}}"
  [ -x "${server_bin}" ] || return 1
  "${server_bin}" --version >/dev/null 2>&1
}

download_setup_file() {
  url="$1"
  destination="$2"
  fallback_python="${3:-}"
  if command -v curl >/dev/null 2>&1; then
    curl --location --fail --silent --show-error --output "${destination}" "${url}"
  elif command -v wget >/dev/null 2>&1; then
    wget --quiet --output-document="${destination}" "${url}"
  elif [ "${fallback_python}" ]; then
    "${fallback_python}" -c 'import pathlib, sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], pathlib.Path(sys.argv[2]))' "${url}" "${destination}"
  else
    fail "Downloading a setup compatibility file requires curl or wget."
  fi
}

rocm_root_linker_missing_legacy_libxml2() {
  rocm_root="$1"
  [ -d "${rocm_root}" ] || return 1
  for linker in "${rocm_root}/lib/llvm/bin/lld" "${rocm_root}/lib/llvm/bin/ld.lld"; do
    if [ -x "${linker}" ] && ldd "${linker}" 2>/dev/null | grep -q 'libxml2\.so\.2 => not found'; then
      return 0
    fi
  done
  return 1
}

rocm_linker_missing_legacy_libxml2() {
  if [ "${TATER_ROCM_PATH:-}" ] && rocm_root_linker_missing_legacy_libxml2 "${TATER_ROCM_PATH}"; then
    return 0
  fi
  if command -v hipconfig >/dev/null 2>&1; then
    hipconfig_root="$(hipconfig --path 2>/dev/null | head -n 1)"
    if [ "${hipconfig_root}" ] && rocm_root_linker_missing_legacy_libxml2 "${hipconfig_root}"; then
      return 0
    fi
  fi
  if command -v hipcc >/dev/null 2>&1 && command -v readlink >/dev/null 2>&1; then
    hipcc_path="$(readlink -f "$(command -v hipcc)" 2>/dev/null || true)"
    if [ "${hipcc_path}" ]; then
      hipcc_root="$(CDPATH= cd "$(dirname "${hipcc_path}")/.." 2>/dev/null && pwd -P || true)"
      if [ "${hipcc_root}" ] && rocm_root_linker_missing_legacy_libxml2 "${hipcc_root}"; then
        return 0
      fi
    fi
  fi
  for rocm_root in /opt/rocm /opt/rocm-*; do
    if rocm_root_linker_missing_legacy_libxml2 "${rocm_root}"; then
      return 0
    fi
  done
  return 1
}

prepare_rocm_linker_compat() {
  rocm_compat_enabled="${TATER_SETUP_ROCM_LIBXML2_COMPAT:-1}"
  rocm_compat_lib_dir="${ROCM_LIBXML2_COMPAT_DIR}/usr/lib/x86_64-linux-gnu"
  if ! rocm_linker_missing_legacy_libxml2; then
    return
  fi
  if [ -e "${rocm_compat_lib_dir}/libxml2.so.2" ]; then
    rocm_compat_lib_dir="$(CDPATH= cd "${rocm_compat_lib_dir}" && pwd -P)"
    LD_LIBRARY_PATH="${rocm_compat_lib_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    export LD_LIBRARY_PATH
    rocm_linker_missing_legacy_libxml2 && fail "The existing ROCm compatibility library could not satisfy libxml2.so.2."
    ok "Using isolated ROCm linker compatibility library"
    return
  fi

  os_id=""
  os_version=""
  if [ -r /etc/os-release ]; then
    os_id="$(. /etc/os-release; printf '%s' "${ID:-}")"
    os_version="$(. /etc/os-release; printf '%s' "${VERSION_ID:-}")"
  fi
  if [ "${os_id}" != "ubuntu" ] || [ "${os_version}" != "26.04" ] || [ "$(uname -m)" != "x86_64" ]; then
    warn "The ROCm linker requires libxml2.so.2, but this setup can only provide the isolated compatibility library on Ubuntu 26.04 x86_64."
    return
  fi
  if ! truthy_env "${rocm_compat_enabled}"; then
    warn "ROCm linker compatibility setup is disabled by TATER_SETUP_ROCM_LIBXML2_COMPAT."
    return
  fi
  command -v sha256sum >/dev/null 2>&1 || fail "ROCm compatibility setup requires sha256sum."
  command -v dpkg-deb >/dev/null 2>&1 || fail "ROCm compatibility setup requires dpkg-deb."

  info "Preparing isolated Ubuntu 26.04 compatibility library for the ROCm linker"
  mkdir -p "${ROCM_LIBXML2_COMPAT_DIR}"
  compat_archive="${ROCM_LIBXML2_COMPAT_DIR}/libxml2-compat.deb"
  download_setup_file "${ROCM_LIBXML2_COMPAT_URL}" "${compat_archive}"
  compat_sha256="$(sha256sum "${compat_archive}" | awk '{print $1}')"
  if [ "${compat_sha256}" != "${ROCM_LIBXML2_COMPAT_SHA256}" ]; then
    rm -f "${compat_archive}"
    fail "ROCm compatibility download checksum did not match."
  fi
  dpkg-deb -x "${compat_archive}" "${ROCM_LIBXML2_COMPAT_DIR}"
  [ -e "${rocm_compat_lib_dir}/libxml2.so.2" ] || fail "ROCm compatibility library was not present in the verified package."

  rocm_compat_lib_dir="$(CDPATH= cd "${rocm_compat_lib_dir}" && pwd -P)"
  LD_LIBRARY_PATH="${rocm_compat_lib_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  export LD_LIBRARY_PATH
  rocm_linker_missing_legacy_libxml2 && fail "The ROCm linker still cannot find libxml2.so.2 after compatibility setup."
  ok "ROCm linker compatibility library is ready"
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
      if [ "${TATER_LLAMA_CPP_ROCM_BACKEND_SELECTED:-hip}" = "vulkan" ]; then
        printf '%s' "-DGGML_VULKAN=on -DGGML_HIP=off"
      else
        printf '%s' "-DGGML_HIP=on -DGGML_VULKAN=off"
      fi
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

handle_llama_cpp_build_failure() {
  message="$1"
  if truthy_env "${TATER_SETUP_REQUIRE_LOCAL_LLM:-1}"; then
    fail "${message}"
  fi
  warn "${message}"
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
  command -v git >/dev/null 2>&1 || { handle_llama_cpp_build_failure "git was not found, so llama.cpp could not be installed."; return; }
  command -v cmake >/dev/null 2>&1 || { handle_llama_cpp_build_failure "cmake was not found, so llama.cpp could not be built."; return; }
  mkdir -p "${RUNTIME_DIR}"
  if [ ! -d "${LLAMA_CPP_DIR}/.git" ]; then
    info "Cloning native llama.cpp runtime"
    git clone --depth 1 --filter=blob:none --no-checkout "${LLAMA_CPP_REPO}" "${LLAMA_CPP_DIR}" || { handle_llama_cpp_build_failure "Could not clone llama.cpp."; return; }
  else
    info "Updating native llama.cpp runtime"
  fi
  git -C "${LLAMA_CPP_DIR}" fetch --depth 1 origin "${LLAMA_CPP_REF}" || { handle_llama_cpp_build_failure "Could not fetch llama.cpp revision ${LLAMA_CPP_REF}."; return; }
  git -C "${LLAMA_CPP_DIR}" checkout --detach FETCH_HEAD >/dev/null 2>&1 || { handle_llama_cpp_build_failure "Could not check out llama.cpp revision ${LLAMA_CPP_REF}."; return; }
  if [ "${profile}" = "rocm" ] && [ "${TATER_LLAMA_CPP_ROCM_BACKEND_SELECTED:-hip}" = "hip" ]; then
    prepare_rocm_linker_compat
  fi
  cmake_args="$(llama_cpp_native_cmake_args "${profile}")"
  cuda_stub_dir="$(llama_cpp_cuda_stub_dir "${profile}")"
  info "Building native llama-server${cmake_args:+ (${cmake_args})}"
  if [ "${cuda_stub_dir}" ]; then
    info "Using CUDA driver stubs for llama.cpp link: ${cuda_stub_dir}"
    # shellcheck disable=SC2086
    cmake -S "${LLAMA_CPP_DIR}" -B "${LLAMA_CPP_DIR}/build" -DCMAKE_BUILD_TYPE=Release ${cmake_args} \
      "-DCMAKE_EXE_LINKER_FLAGS=-L${cuda_stub_dir} -Wl,-rpath-link,${cuda_stub_dir}" \
      "-DCMAKE_SHARED_LINKER_FLAGS=-L${cuda_stub_dir} -Wl,-rpath-link,${cuda_stub_dir}" || { handle_llama_cpp_build_failure "llama.cpp configure failed for ${profile}."; return; }
  else
    # shellcheck disable=SC2086
    cmake -S "${LLAMA_CPP_DIR}" -B "${LLAMA_CPP_DIR}/build" -DCMAKE_BUILD_TYPE=Release ${cmake_args} || { handle_llama_cpp_build_failure "llama.cpp configure failed for ${profile}."; return; }
  fi
  cmake --build "${LLAMA_CPP_DIR}/build" --config Release --target llama-server -j "${TATER_LLAMA_CPP_BUILD_JOBS:-4}" || { handle_llama_cpp_build_failure "llama-server build failed for ${profile}."; return; }
  if check_llama_cpp_native "${LLAMA_CPP_SERVER_BIN}"; then
    ok "Built native llama.cpp server at ${LLAMA_CPP_SERVER_BIN}"
  else
    handle_llama_cpp_build_failure "llama-server build finished, but ${LLAMA_CPP_SERVER_BIN} was not executable."
  fi
}

install_mlx_engine_checkout() {
  if [ "${TATER_SETUP_MLX_ENGINE:-1}" = "0" ]; then
    warn "Skipping MLX engine checkout because TATER_SETUP_MLX_ENGINE=0."
    return
  fi
  if [ "${TATER_MLX_ENGINE_PATH:-}" ] && [ -d "${TATER_MLX_ENGINE_PATH}/mlx_engine" ]; then
    ok "Using MLX engine at ${TATER_MLX_ENGINE_PATH}"
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
      warn "Homebrew was not found. If native installs fail, install Homebrew packages: ffmpeg cmake."
    else
      warn "If native package builds fail, run: brew install ffmpeg cmake"
      warn "Matrix encryption and embedded Redis use bundled Apple Silicon wheels; source fallback may still need native build tools."
    fi
  fi
  info "Installing Tater dependencies for macOS"
  install_macos_bundled_native_wheels "${venv_python}"
  pip_install_requirements "${venv_python}" "${tmp_req}"
  install_llama_cpp_native macos
  info "Installing Apple-native speech extras"
  if ! "${venv_python}" -m pip install mlx-whisper kokoro; then
    warn "Apple-native speech extras failed to install. Tater will still run with Faster Whisper/Kokoro CPU fallbacks."
  fi
  if [ "${is_apple_silicon}" = "1" ]; then
    install_mlx_engine_checkout
    mlx_engine_requirements="${TATER_MLX_ENGINE_PATH:-${RUNTIME_DIR}/mlx-engine}/requirements.txt"
    if [ -f "${mlx_engine_requirements}" ]; then
      info "Aligning Python packages with the selected MLX engine"
      mlx_engine_runtime_requirements="$(mktemp "${TMPDIR:-/tmp}/tater-mlx-engine-requirements.XXXXXX")"
      awk '
        /^(mlx|mlx-metal)==/ ||
        /^(mlx-lm|mlx-vlm|outlines) @/ ||
        /^(outlines-core|dill|xxhash)==/ { print }
      ' "${mlx_engine_requirements}" > "${mlx_engine_runtime_requirements}"
      if [ -s "${mlx_engine_runtime_requirements}" ]; then
        pip_install_requirements "${venv_python}" "${mlx_engine_runtime_requirements}"
      fi
      rm -f "${mlx_engine_runtime_requirements}"
    fi
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
  pip_install_requirements "${venv_python}" "${tmp_req}"
  info "Installing NVIDIA TensorFlow CUDA extras for Face ID"
  "${venv_python}" -m pip install --upgrade "tensorflow[and-cuda]==2.21.0"
  install_llama_cpp_native nvidia
  info "Switching ONNX Runtime to GPU build"
  "${venv_python}" -m pip uninstall -y onnxruntime >/dev/null 2>&1 || true
  "${venv_python}" -m pip install "onnxruntime-gpu[cuda,cudnn]<1.27"
  rm -f "${tmp_req}"
  trap - EXIT
}

install_rocm() {
  venv_python="$1"
  tmp_req="$(mktemp "${TMPDIR:-/tmp}/tater-requirements-rocm.XXXXXX")"
  trap 'rm -f "${tmp_req}"' EXIT
  filtered_requirements "${tmp_req}"

  warn "AMD ROCm support is Linux-only and depends on the ROCm runtime installed for your GPU/APU."
  if rocm_torch_ready "${venv_python}" && ! truthy_env "${TATER_SETUP_UPGRADE_ROCM:-}"; then
    ok "Using the existing working PyTorch ROCm installation"
  elif [ -z "${TATER_ROCM_PYTORCH_INDEX_URL:-}" ] && is_amd_ryzen_ai_host && [ "$(python_version "${venv_python}")" = "3.12" ]; then
    install_amd_ryzen_ai_pytorch "${venv_python}"
  else
    rocm_index="${TATER_ROCM_PYTORCH_INDEX_URL:-https://download.pytorch.org/whl/rocm6.4}"
    warn "Generic AMD systems use the PyTorch ROCm 6.4 index by default; override TATER_ROCM_PYTORCH_INDEX_URL when the installed ROCm stack requires another version."
    info "Installing AMD ROCm PyTorch wheels from ${rocm_index}"
    "${venv_python}" -m pip install --index-url "${rocm_index}" torch torchaudio
  fi
  info "Installing Tater dependencies"
  pip_install_requirements "${venv_python}" "${tmp_req}"
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
  pip_install_requirements "${venv_python}" "${tmp_req}"
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
  strix_halo_full_offload="0"
  rocm_gfx_target=""
  llama_cpp_gpu_backend=""
  case "${profile}" in
    edge)
      speech_acceleration="cpu"
      ;;
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
      if is_amd_ryzen_ai_host; then
        rocm_gfx_target="$(amd_ryzen_ai_gfx_target)"
      else
        rocm_gfx_target="${TATER_ROCM_GFX_TARGET:-}"
      fi
      llama_cpp_gpu_backend="${TATER_LLAMA_CPP_ROCM_BACKEND_SELECTED:-auto}"
      if is_strix_halo_host; then
        strix_halo_full_offload="1"
      fi
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
    if [ "${AIRPLAY_SENDER_BIN:-}" ]; then
      say "export TATER_AIRPLAY_CLI_PATH=\"\${TATER_AIRPLAY_CLI_PATH:-${AIRPLAY_SENDER_BIN}}\""
    fi
    if [ "${AIRPLAY_RECEIVER_BIN:-}" ]; then
      say "export TATER_SHAIRPORT_SYNC_PATH=\"\${TATER_SHAIRPORT_SYNC_PATH:-${AIRPLAY_RECEIVER_BIN}}\""
    fi
    if [ "${AIRPLAY_FFMPEG_BIN:-}" ]; then
      say "export TATER_FFMPEG_PATH=\"\${TATER_FFMPEG_PATH:-${AIRPLAY_FFMPEG_BIN}}\""
    fi
    if [ "${profile}" = "edge" ]; then
      say "export TATER_REMOTE_ONLY=\"\${TATER_REMOTE_ONLY:-1}\""
      say "export TATER_SETUP_LLAMA_CPP_NATIVE=\"\${TATER_SETUP_LLAMA_CPP_NATIVE:-0}\""
      say "export TATER_SETUP_REQUIRE_LOCAL_LLM=\"\${TATER_SETUP_REQUIRE_LOCAL_LLM:-0}\""
      say "export TATER_RUNTIME_WAKE_WORKERS=\"\${TATER_RUNTIME_WAKE_WORKERS:-1}\""
      say "export TATER_RUNTIME_STT_WORKERS=\"\${TATER_RUNTIME_STT_WORKERS:-1}\""
      say "export TATER_RUNTIME_TTS_WORKERS=\"\${TATER_RUNTIME_TTS_WORKERS:-1}\""
      say "export TATER_RUNTIME_SPEECH_WORKERS=\"\${TATER_RUNTIME_SPEECH_WORKERS:-1}\""
      say "export TATER_RUNTIME_DASHBOARD_WORKERS=\"\${TATER_RUNTIME_DASHBOARD_WORKERS:-1}\""
      say "export TATER_RUNTIME_BACKGROUND_WORKERS=\"\${TATER_RUNTIME_BACKGROUND_WORKERS:-1}\""
    fi
    if [ "${torch_mps_fallback}" ]; then
      say "export PYTORCH_ENABLE_MPS_FALLBACK=\"\${PYTORCH_ENABLE_MPS_FALLBACK:-${torch_mps_fallback}}\""
    fi
    if [ "${profile}" = "nvidia" ]; then
      say "export TATER_LLAMA_CPP_N_GPU_LAYERS=\"\${TATER_LLAMA_CPP_N_GPU_LAYERS:-auto}\""
      if [ "${nvidia_site_packages}" ]; then
        say "export LD_LIBRARY_PATH=\"${nvidia_site_packages}/nvidia/cublas/lib:${nvidia_site_packages}/nvidia/cuda_runtime/lib:${nvidia_site_packages}/nvidia/cuda_nvrtc/lib:${nvidia_site_packages}/nvidia/cudnn/lib:${nvidia_site_packages}/nvidia/curand/lib:${nvidia_site_packages}/nvidia/cusolver/lib:${nvidia_site_packages}/nvidia/cusparse/lib:${nvidia_site_packages}/nvidia/nvjitlink/lib:\${LD_LIBRARY_PATH:-}\""
      fi
    fi
    if [ "${profile}" = "rocm" ]; then
      say "export TATER_ROCM_GFX_TARGET=\"\${TATER_ROCM_GFX_TARGET:-${rocm_gfx_target}}\""
      say "export TATER_LLAMA_CPP_GPU_BACKEND=\"\${TATER_LLAMA_CPP_GPU_BACKEND:-${llama_cpp_gpu_backend}}\""
    fi
    if [ "${strix_halo_full_offload}" = "1" ]; then
      say "export TATER_LLAMA_CPP_N_GPU_LAYERS=\"\${TATER_LLAMA_CPP_N_GPU_LAYERS:-all}\""
      say "export TATER_LLAMA_CPP_DRAFT_N_GPU_LAYERS=\"\${TATER_LLAMA_CPP_DRAFT_N_GPU_LAYERS:-all}\""
    fi
  } > "${PROFILE_ENV}"

  if [ "${strix_halo_full_offload}" = "1" ]; then
    ok "Detected Strix Halo; llama.cpp target and draft models will fully offload to ROCm VRAM"
  fi

  printf '%s\n' "${profile}" > "${PROFILE_FILE}"
}

verify_install() {
  venv_python="$1"
  profile="$2"
  info "Checking installed runtime"
  require_local_llm_default="1"
  remote_only_default="0"
  if [ "${profile}" = "edge" ]; then
    require_local_llm_default="0"
    remote_only_default="1"
  fi
  TATER_LLAMA_CPP_SERVER_BIN="${LLAMA_CPP_SERVER_BIN}" \
  TATER_SETUP_PROFILE="${profile}" \
  TATER_SETUP_REQUIRE_LOCAL_LLM="${TATER_SETUP_REQUIRE_LOCAL_LLM:-${require_local_llm_default}}" \
  TATER_REMOTE_ONLY="${TATER_REMOTE_ONLY:-${remote_only_default}}" \
  TATER_MLX_ENGINE_PATH="${TATER_MLX_ENGINE_PATH:-}" \
  TATER_RUNTIME_DIR="${RUNTIME_DIR}" \
  TATER_AIRPLAY_CLI_PATH="${AIRPLAY_SENDER_BIN:-${TATER_AIRPLAY_CLI_PATH:-}}" \
  TATER_SHAIRPORT_SYNC_PATH="${AIRPLAY_RECEIVER_BIN:-${TATER_SHAIRPORT_SYNC_PATH:-}}" \
  TATER_FFMPEG_PATH="${AIRPLAY_FFMPEG_BIN:-${TATER_FFMPEG_PATH:-}}" \
  "${venv_python}" - <<'PY'
import importlib.util
import os
import platform
import subprocess
import sys
from pathlib import Path

required = ["fastapi", "uvicorn", "redis"]
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    raise SystemExit("Missing required packages: " + ", ".join(missing))

import shutil
redis_server = shutil.which("redis-server")
embedded_redis = importlib.util.find_spec("redislite") is not None
if not redis_server and not embedded_redis:
    raise SystemExit("Edge profile requires the operating system redis-server package")
print(f"redis_runtime={'system:' + redis_server if redis_server else 'redislite'}")

print("core imports ok")

from airplay_bridge import _find_ffmpeg, ensure_airplay_cli
from external_audio import SHAIRPORT_SYNC_VERSION, _find_shairport_sync

ffmpeg = _find_ffmpeg()
if not ffmpeg:
    raise SystemExit("AirPlay FFmpeg runtime is unavailable")
completed = subprocess.run([ffmpeg, "-version"], text=True, capture_output=True, timeout=10)
if completed.returncode != 0:
    raise SystemExit("AirPlay FFmpeg runtime failed its self-check")
print(f"airplay_ffmpeg={ffmpeg}")

sender = ensure_airplay_cli()
print(f"airplay_sender={sender}")

receiver = _find_shairport_sync()
if not receiver:
    raise SystemExit("Pinned Shairport Sync receiver is unavailable")
version_check = subprocess.run([receiver, "-V"], text=True, capture_output=True, timeout=10)
version_output = (version_check.stdout or "") + (version_check.stderr or "")
if version_check.returncode != 0 or not version_output.strip().startswith(f"{SHAIRPORT_SYNC_VERSION}-"):
    raise SystemExit(f"Shairport Sync {SHAIRPORT_SYNC_VERSION} is required")
help_check = subprocess.run([receiver, "-h"], text=True, capture_output=True, timeout=10)
help_output = (help_check.stdout or "") + (help_check.stderr or "")
if "--service-type" not in help_output or "stdout" not in help_output:
    raise SystemExit("Shairport Sync is missing classic/stdout receiver support")
print(f"airplay_receiver={receiver}")

remote_only = str(os.getenv("TATER_REMOTE_ONLY") or "").strip().lower() in ("1", "true", "yes", "on")
setup_profile = str(os.getenv("TATER_SETUP_PROFILE") or "").strip().lower()
if remote_only:
    forbidden = [
        "torch",
        "tensorflow",
        "deepface",
        "faster_whisper",
        "transformers",
        "onnx_asr",
        "speechbrain",
    ]
    unexpectedly_installed = [name for name in forbidden if importlib.util.find_spec(name) is not None]
    if unexpectedly_installed:
        raise SystemExit("Remote-only profile unexpectedly installed local model packages: " + ", ".join(unexpectedly_installed))
    print("remote_only_dependency_boundary ok")
else:
    face_id_required = ["cv2", "deepface", "retinaface", "tensorflow", "tf_keras", "torch", "transformers", "huggingface_hub", "omegaconf", "fvcore"]
    face_id_missing = [name for name in face_id_required if importlib.util.find_spec(name) is None]
    if face_id_missing:
        raise SystemExit("Missing Face ID packages: " + ", ".join(face_id_missing))
    print("face_id_imports ok")

cuda = False
hip = None
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

if setup_profile == "rocm" and not (cuda and hip):
    raise SystemExit(
        "The AMD profile installed, but PyTorch cannot access the GPU. Verify that the installed "
        "ROCm version supports this processor, that /dev/kfd is available, and that the current "
        "user belongs to the render and video groups; a reboot may be required after driver changes."
    )

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
require_raw = str(os.getenv("TATER_SETUP_REQUIRE_LOCAL_LLM") or "1").strip().lower()
require_local_llm = require_raw not in ("", "0", "false", "no", "off")
if server_bin:
    server_path = Path(server_bin)
    server_available = server_path.is_file() and os.access(server_bin, os.X_OK)
    if not server_available:
        message = (
            f"Missing required llama.cpp server binary: {server_bin}. "
            "Review the earlier native build error and rerun setup, or set "
            "TATER_SETUP_LLAMA_CPP_NATIVE=0 TATER_SETUP_REQUIRE_LOCAL_LLM=0 "
            "to skip Tater's built-in local LLM runtime."
        )
        print(f"llama_server unavailable: {message}")
        if require_local_llm:
            raise SystemExit(message)
    if server_available:
        try:
            completed = subprocess.run([server_bin, "--version"], text=True, capture_output=True, timeout=10)
            output = " ".join(((completed.stdout or "") + " " + (completed.stderr or "")).split())
            print(f"llama_server={server_bin} {output}")
            if completed.returncode != 0 and require_local_llm:
                raise SystemExit(f"llama_server failed: {output}")
        except Exception as exc:
            print(f"llama_server unavailable: {exc}")
            if require_local_llm:
                raise
else:
    print("llama_server unavailable: TATER_LLAMA_CPP_SERVER_BIN is not set")
    if require_local_llm:
        raise SystemExit("Missing required llama.cpp server binary")

if require_local_llm:
    llm_modules = ["transformers", "accelerate"]
    is_apple_silicon = platform.system() == "Darwin" and platform.machine() == "arm64"
    if is_apple_silicon:
        llm_modules.extend(["mlx_lm", "mlx_vlm", "outlines", "outlines_core", "dill", "xxhash"])
    missing_llm = [name for name in llm_modules if importlib.util.find_spec(name) is None]
    if missing_llm:
        raise SystemExit("Missing required local LLM packages: " + ", ".join(missing_llm))
    print("local_llm_imports ok")
    if is_apple_silicon:
        mlx_engine = os.getenv("TATER_MLX_ENGINE_PATH") or str(Path(os.getenv("TATER_RUNTIME_DIR", ".runtime")) / "mlx-engine")
        if not (Path(mlx_engine) / "mlx_engine").is_dir():
            raise SystemExit(f"Missing required MLX engine checkout: {mlx_engine}")
        if mlx_engine not in sys.path:
            sys.path.insert(0, mlx_engine)
        try:
            from mlx_engine.generate import create_generator, load_model, tokenize
        except Exception as exc:
            raise SystemExit(f"MLX engine import failed: {type(exc).__name__}: {exc}") from exc
        print(f"mlx_engine_imports ok path={mlx_engine}")

for name in ("mlx_whisper", "kokoro"):
    try:
        __import__(name)
        print(f"{name}=available")
    except Exception as exc:
        print(f"{name}=unavailable: {exc}")

if remote_only:
    import tateros_app
    from speech_settings import DEFAULT_STT_BACKEND
    from tater_voice.voice_pipeline import DEFAULT_VAD_BACKEND

    if DEFAULT_STT_BACKEND != "wyoming":
        raise SystemExit(f"Remote-only STT default is {DEFAULT_STT_BACKEND}, expected wyoming")
    if DEFAULT_VAD_BACKEND != "webrtc":
        raise SystemExit(f"Remote-only VAD default is {DEFAULT_VAD_BACKEND}, expected webrtc")
    print(f"edge_app_import ok module={tateros_app.__name__} stt={DEFAULT_STT_BACKEND} vad={DEFAULT_VAD_BACKEND}")
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

  required_python_minor=""
  if [ "${profile}" = "rocm" ] && [ -z "${TATER_ROCM_PYTORCH_INDEX_URL:-}" ] && is_amd_ryzen_ai_host && ! existing_rocm_environment_ready; then
    required_python_minor="3.12"
    info "Ryzen AI ROCm setup uses AMD's ROCm ${AMD_RYZEN_AI_ROCM_VERSION} package set with Python 3.12"
    warn_if_unvalidated_ryzen_ai_os
  fi
  if [ "${profile}" = "rocm" ] && existing_rocm_environment_ready; then
    detected_python="${VENV_DIR}/bin/python"
  else
    detected_python="$(find_python "${required_python_minor}")"
  fi
  select_supported_python "${detected_python}" "${required_python_minor}"
  python_bin="${SUPPORTED_PYTHON_BIN}"

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
  elif [ "${profile}" = "edge" ]; then
    warn "Edge profile disables local model runtimes. Pair it as a Spudlet to use Spud Hub model routing, or configure standalone remote providers in TaterOS."
  fi

  if [ "${profile}" = "rocm" ]; then
    ensure_amd_gpu_device_access
  fi
  ensure_linux_build_tools "${profile}"
  ensure_venv "${profile}" "${python_bin}" "${required_python_minor}"
  venv_python="${VENV_DIR}/bin/python"
  install_base "${venv_python}"

  case "${profile}" in
    edge) install_edge "${venv_python}" ;;
    cpu) install_cpu "${venv_python}" ;;
    macos) install_macos "${venv_python}" ;;
    nvidia) install_nvidia "${venv_python}" ;;
    rocm) install_rocm "${venv_python}" ;;
    jetson|thor) install_jetson_like "${venv_python}" "${profile}" ;;
  esac

  install_airplay_runtime_dependencies "${profile}" "${venv_python}"
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
