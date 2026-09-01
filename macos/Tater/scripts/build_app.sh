#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd "$(dirname "$0")" && pwd -P)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
REPO_ROOT="$(cd "${PROJECT_DIR}/../.." && pwd -P)"
APP_NAME="Tater"
APP_DIR="${PROJECT_DIR}/build/${APP_NAME}.app"
LEGACY_APP_DIR="${PROJECT_DIR}/build/TaterAssistant.app"
CONTENTS_DIR="${APP_DIR}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
RESOURCES_DIR="${CONTENTS_DIR}/Resources"
SOURCE_SNAPSHOT_DIR="${RESOURCES_DIR}/TaterSource"
NATIVE_RESOURCES_DIR="${RESOURCES_DIR}/Native"
CODESIGN_IDENTITY="${TATER_CODESIGN_IDENTITY:--}"
CODESIGN_ENTITLEMENTS="${TATER_CODESIGN_ENTITLEMENTS:-${PROJECT_DIR}/Resources/Tater.entitlements}"
LLAMA_CPP_REPO="${TATER_LLAMA_CPP_REPO:-https://github.com/ggml-org/llama.cpp.git}"
LLAMA_CPP_REF="${TATER_LLAMA_CPP_REF:-fe2120bc9db242c4349a6f71810af1cd52ee8580}"
NATIVE_BUILD_DIR="${PROJECT_DIR}/build/native"
LLAMA_CPP_DIR="${TATER_MACOS_LLAMA_CPP_DIR:-${NATIVE_BUILD_DIR}/llama.cpp}"
MLX_ENGINE_DIR="${TATER_MACOS_MLX_ENGINE_DIR:-${NATIVE_BUILD_DIR}/mlx-engine}"
MACOS_DEPLOYMENT_TARGET="${TATER_MACOS_DEPLOYMENT_TARGET:-15.0}"

# Keep SwiftPM and native CMake builds on the same deployment target. Without
# this, native libraries can inherit the release machine's current macOS SDK
# target and fail to launch on otherwise supported macOS versions.
export MACOSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}"

swift build -c release --package-path "${PROJECT_DIR}"
BIN_DIR="$(swift build -c release --package-path "${PROJECT_DIR}" --show-bin-path)"

"${SCRIPT_DIR}/generate_app_icon.sh"

rm -rf "${APP_DIR}" "${LEGACY_APP_DIR}"
mkdir -p "${MACOS_DIR}" "${RESOURCES_DIR}"

cp "${BIN_DIR}/TaterAssistant" "${MACOS_DIR}/TaterAssistant"
cp "${PROJECT_DIR}/Resources/Info.plist" "${CONTENTS_DIR}/Info.plist"
/usr/libexec/PlistBuddy \
  -c "Set :LSMinimumSystemVersion ${MACOS_DEPLOYMENT_TARGET}" \
  "${CONTENTS_DIR}/Info.plist"
cp "${PROJECT_DIR}/Resources/TaterIcon.icns" "${RESOURCES_DIR}/TaterIcon.icns"
cp "${PROJECT_DIR}/Resources/TaterMenuBarTemplate.png" "${RESOURCES_DIR}/TaterMenuBarTemplate.png"
cp "${PROJECT_DIR}/Resources/TaterAvatar.png" "${RESOURCES_DIR}/TaterAvatar.png"
cp "${PROJECT_DIR}/Resources/TaterSetupLogo.png" "${RESOURCES_DIR}/TaterSetupLogo.png"
cp "${PROJECT_DIR}/Resources/TaterMascotSprite.png" "${RESOURCES_DIR}/TaterMascotSprite.png"
mkdir -p "${RESOURCES_DIR}/TaterMascotFrames"
cp "${PROJECT_DIR}/Resources/TaterMascotFrames/"*.png "${RESOURCES_DIR}/TaterMascotFrames/"
mkdir -p "${RESOURCES_DIR}/TaterMascotIdleFrames"
cp "${PROJECT_DIR}/Resources/TaterMascotIdleFrames/"*.png "${RESOURCES_DIR}/TaterMascotIdleFrames/"
rsync -a --delete \
  --exclude='/.git/' \
  --exclude='/.github/' \
  --exclude='/.agents/' \
  --exclude='/.codex/' \
  --exclude='/.venv/' \
  --exclude='/.runtime/' \
  --exclude='/agent_lab/' \
  --exclude='/cores/' \
  --exclude='/integrations/' \
  --exclude='/macos/' \
  --exclude='/portals/' \
  --exclude='/verba/' \
  --exclude='/wav2vec2_checkpoints/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  --exclude='.DS_Store' \
  "${REPO_ROOT}/" "${SOURCE_SNAPSHOT_DIR}/"

sign_bundled_wheel_payloads() {
  wheel_dir="${SOURCE_SNAPSHOT_DIR}/vendor/wheels/macos"
  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Darwin" ] || [ ! -d "${wheel_dir}" ]; then
    return
  fi

  for wheel_path in "${wheel_dir}"/*.whl; do
    [ -f "${wheel_path}" ] || continue
    work_dir="$(mktemp -d "${TMPDIR:-/tmp}/tater-wheel-sign.XXXXXX")"
    unpack_dir="${work_dir}/wheel"
    mkdir -p "${unpack_dir}"
    unzip -q "${wheel_path}" -d "${unpack_dir}"

    find "${unpack_dir}" -type f | while IFS= read -r payload_path; do
      if file "${payload_path}" | grep -q 'Mach-O'; then
        sign_mach_o_payload "${payload_path}"
      fi
    done

    /usr/bin/python3 - "${unpack_dir}" <<'PY'
import base64
import csv
import hashlib
import os
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
record_paths = sorted(root.glob("*.dist-info/RECORD"))
if not record_paths:
    raise SystemExit("wheel RECORD not found")
record = record_paths[0]
rows = []
for path in sorted(p for p in root.rglob("*") if p.is_file()):
    rel = path.relative_to(root).as_posix()
    if path == record:
        rows.append([rel, "", ""])
        continue
    data = path.read_bytes()
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode("ascii")
    rows.append([rel, f"sha256={digest}", str(len(data))])
with record.open("w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerows(rows)
PY

    rm -f "${wheel_path}"
    (cd "${unpack_dir}" && /usr/bin/zip -qr "${wheel_path}" .)
    rm -rf "${work_dir}"
  done
}

sign_mach_o_payload() {
  payload_path="$1"
  if [ "${CODESIGN_IDENTITY}" = "-" ]; then
    codesign --force --sign "${CODESIGN_IDENTITY}" "${payload_path}"
  else
    codesign --force --options runtime --timestamp --sign "${CODESIGN_IDENTITY}" "${payload_path}"
  fi
}

sign_native_runtime_payloads() {
  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Darwin" ] || [ ! -d "${NATIVE_RESOURCES_DIR}" ]; then
    return
  fi
  find "${NATIVE_RESOURCES_DIR}" -type f | while IFS= read -r payload_path; do
    if file "${payload_path}" | grep -q 'Mach-O'; then
      sign_mach_o_payload "${payload_path}"
    fi
  done
}

sign_bundled_wheel_payloads

run_with_retries() {
  attempts="${TATER_GIT_RETRY_ATTEMPTS:-4}"
  delay="${TATER_GIT_RETRY_DELAY:-5}"
  attempt=1
  while :; do
    if "$@"; then
      return 0
    fi
    status="$?"
    if [ "${attempt}" -ge "${attempts}" ]; then
      return "${status}"
    fi
    printf 'Command failed with status %s; retrying in %ss (%s/%s): %s\n' \
      "${status}" "${delay}" "${attempt}" "${attempts}" "$*" >&2
    sleep "${delay}"
    attempt=$((attempt + 1))
    delay=$((delay * 2))
  done
}

clone_with_retries() {
  destination="$1"
  shift
  attempts="${TATER_GIT_RETRY_ATTEMPTS:-4}"
  delay="${TATER_GIT_RETRY_DELAY:-5}"
  attempt=1
  while :; do
    rm -rf "${destination}"
    if "$@" "${destination}"; then
      return 0
    fi
    status="$?"
    if [ "${attempt}" -ge "${attempts}" ]; then
      return "${status}"
    fi
    printf 'Clone failed with status %s; retrying in %ss (%s/%s): %s\n' \
      "${status}" "${delay}" "${attempt}" "${attempts}" "$*" >&2
    sleep "${delay}"
    attempt=$((attempt + 1))
    delay=$((delay * 2))
  done
}

prepare_bundled_llama_cpp_runtime() {
  if [ "${TATER_BUNDLE_LLAMA_CPP:-1}" = "0" ]; then
    return
  fi
  command -v git >/dev/null 2>&1 || { printf 'git is required to prepare bundled llama.cpp runtime.\n' >&2; exit 1; }
  command -v cmake >/dev/null 2>&1 || { printf 'cmake is required to prepare bundled llama.cpp runtime.\n' >&2; exit 1; }

  mkdir -p "${NATIVE_BUILD_DIR}"
  if [ ! -d "${LLAMA_CPP_DIR}/.git" ]; then
    clone_with_retries "${LLAMA_CPP_DIR}" git clone --depth 1 --filter=blob:none --no-checkout "${LLAMA_CPP_REPO}"
  fi
  run_with_retries git -C "${LLAMA_CPP_DIR}" fetch --depth 1 origin "${LLAMA_CPP_REF}"
  git -C "${LLAMA_CPP_DIR}" checkout --detach FETCH_HEAD >/dev/null 2>&1

  # Avoid packaging stale dylibs left behind by earlier llama.cpp versions.
  rm -rf "${LLAMA_CPP_DIR}/build"
  cmake \
    -S "${LLAMA_CPP_DIR}" \
    -B "${LLAMA_CPP_DIR}/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOS_DEPLOYMENT_TARGET}" \
    -DGGML_METAL=on \
    -DLLAMA_OPENSSL=OFF \
    -DLLAMA_BUILD_UI=OFF \
    -DLLAMA_USE_PREBUILT_UI=OFF \
    -DLLAMA_BUILD_TESTS=OFF
  cmake --build "${LLAMA_CPP_DIR}/build" --config Release --target llama-server -j "${TATER_LLAMA_CPP_BUILD_JOBS:-4}"

  build_bin="${LLAMA_CPP_DIR}/build/bin"
  bundled_bin="${NATIVE_RESOURCES_DIR}/llama.cpp/bin"
  test -x "${build_bin}/llama-server"
  rm -rf "${NATIVE_RESOURCES_DIR}/llama.cpp"
  mkdir -p "${bundled_bin}"
  rsync -a \
    --include='/llama-server' \
    --include='/*.dylib' \
    --exclude='*' \
    "${build_bin}/" "${bundled_bin}/"

  for mach_o in "${bundled_bin}/llama-server" "${bundled_bin}"/*.dylib; do
    [ -e "${mach_o}" ] || continue
    otool -l "${mach_o}" 2>/dev/null | awk '
      $1 == "path" {
        print $2
      }
    ' | while IFS= read -r rpath; do
      case "${rpath}" in
        @executable_path|@loader_path) ;;
        *) install_name_tool -delete_rpath "${rpath}" "${mach_o}" >/dev/null 2>&1 || true ;;
      esac
    done
    install_name_tool -add_rpath "@executable_path" "${mach_o}" >/dev/null 2>&1 || true
    install_name_tool -add_rpath "@loader_path" "${mach_o}" >/dev/null 2>&1 || true
  done

  DYLD_LIBRARY_PATH="${bundled_bin}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}" "${bundled_bin}/llama-server" --version >/dev/null
}

prepare_bundled_mlx_engine_runtime() {
  if [ "${TATER_BUNDLE_MLX_ENGINE:-1}" = "0" ]; then
    return
  fi
  if [ "$(uname -s 2>/dev/null || printf unknown)" != "Darwin" ] || [ "$(uname -m 2>/dev/null || printf unknown)" != "arm64" ]; then
    return
  fi
  command -v git >/dev/null 2>&1 || { printf 'git is required to prepare bundled MLX engine runtime.\n' >&2; exit 1; }

  mkdir -p "${NATIVE_BUILD_DIR}"
  if [ ! -d "${MLX_ENGINE_DIR}/.git" ]; then
    clone_with_retries "${MLX_ENGINE_DIR}" git clone --depth 1 https://github.com/lmstudio-ai/mlx-engine.git
  else
    run_with_retries git -C "${MLX_ENGINE_DIR}" pull --ff-only
  fi

  bundled_engine="${NATIVE_RESOURCES_DIR}/mlx-engine"
  rm -rf "${bundled_engine}"
  rsync -a \
    --exclude='/.git/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    "${MLX_ENGINE_DIR}/" "${bundled_engine}/"
  test -d "${bundled_engine}/mlx_engine"
}

version_exceeds_target() {
  actual_version="$1"
  target_version="$2"
  awk -v actual="${actual_version}" -v target="${target_version}" 'BEGIN {
    actual_count = split(actual, actual_parts, ".")
    target_count = split(target, target_parts, ".")
    count = actual_count > target_count ? actual_count : target_count
    for (part_index = 1; part_index <= count; part_index++) {
      actual_part = actual_parts[part_index] + 0
      target_part = target_parts[part_index] + 0
      if (actual_part > target_part) exit 0
      if (actual_part < target_part) exit 1
    }
    exit 1
  }'
}

verify_macos_deployment_targets() {
  report_path="$(mktemp "${TMPDIR:-/tmp}/tater-deployment-targets.XXXXXX")"
  find "${APP_DIR}" -type f | while IFS= read -r payload_path; do
    if ! file "${payload_path}" | grep -q 'Mach-O'; then
      continue
    fi
    otool -l "${payload_path}" | awk -v path="${payload_path}" '
      $1 == "cmd" {
        build_version = ($2 == "LC_BUILD_VERSION")
        legacy_version = ($2 == "LC_VERSION_MIN_MACOSX")
        next
      }
      build_version && $1 == "minos" { print $2, path }
      legacy_version && $1 == "version" { print $2, path }
    '
  done > "${report_path}"

  if [ ! -s "${report_path}" ]; then
    rm -f "${report_path}"
    printf 'Could not read a macOS deployment target from the app bundle.\n' >&2
    exit 1
  fi

  invalid_target=0
  while IFS=' ' read -r minimum_version payload_path; do
    if version_exceeds_target "${minimum_version}" "${MACOS_DEPLOYMENT_TARGET}"; then
      printf '%s requires macOS %s, above the supported build target %s.\n' \
        "${payload_path}" "${minimum_version}" "${MACOS_DEPLOYMENT_TARGET}" >&2
      invalid_target=1
    fi
  done < "${report_path}"
  rm -f "${report_path}"

  if [ "${invalid_target}" -ne 0 ]; then
    exit 1
  fi
}

prepare_bundled_llama_cpp_runtime
prepare_bundled_mlx_engine_runtime
verify_macos_deployment_targets

chmod +x "${MACOS_DIR}/TaterAssistant"

find "${APP_DIR}" -exec xattr -c {} +
sign_native_runtime_payloads
if [ "${CODESIGN_IDENTITY}" = "-" ]; then
  codesign --force --deep --sign "${CODESIGN_IDENTITY}" "${APP_DIR}"
else
  if [ ! -f "${CODESIGN_ENTITLEMENTS}" ]; then
    printf 'Missing codesign entitlements: %s\n' "${CODESIGN_ENTITLEMENTS}" >&2
    exit 1
  fi
  codesign \
    --force \
    --deep \
    --options runtime \
    --timestamp \
    --entitlements "${CODESIGN_ENTITLEMENTS}" \
    --sign "${CODESIGN_IDENTITY}" \
    "${APP_DIR}"
fi
codesign --verify --deep --strict --verbose=2 "${APP_DIR}"

printf 'Built %s\n' "${APP_DIR}"
