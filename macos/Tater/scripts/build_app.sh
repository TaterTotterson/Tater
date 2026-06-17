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
CODESIGN_IDENTITY="${TATER_CODESIGN_IDENTITY:--}"
CODESIGN_ENTITLEMENTS="${TATER_CODESIGN_ENTITLEMENTS:-${PROJECT_DIR}/Resources/Tater.entitlements}"

swift build -c release --package-path "${PROJECT_DIR}"
BIN_DIR="$(swift build -c release --package-path "${PROJECT_DIR}" --show-bin-path)"

"${SCRIPT_DIR}/generate_app_icon.sh"

rm -rf "${APP_DIR}" "${LEGACY_APP_DIR}"
mkdir -p "${MACOS_DIR}" "${RESOURCES_DIR}"

cp "${BIN_DIR}/TaterAssistant" "${MACOS_DIR}/TaterAssistant"
cp "${PROJECT_DIR}/Resources/Info.plist" "${CONTENTS_DIR}/Info.plist"
cp "${PROJECT_DIR}/Resources/TaterIcon.icns" "${RESOURCES_DIR}/TaterIcon.icns"
cp "${PROJECT_DIR}/Resources/TaterMenuBarTemplate.png" "${RESOURCES_DIR}/TaterMenuBarTemplate.png"
cp "${PROJECT_DIR}/Resources/TaterAvatar.png" "${RESOURCES_DIR}/TaterAvatar.png"
cp "${PROJECT_DIR}/Resources/TaterSetupLogo.png" "${RESOURCES_DIR}/TaterSetupLogo.png"
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
        if [ "${CODESIGN_IDENTITY}" = "-" ]; then
          codesign --force --sign "${CODESIGN_IDENTITY}" "${payload_path}"
        else
          codesign --force --options runtime --timestamp --sign "${CODESIGN_IDENTITY}" "${payload_path}"
        fi
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

sign_bundled_wheel_payloads

chmod +x "${MACOS_DIR}/TaterAssistant"

find "${APP_DIR}" -exec xattr -c {} +
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
