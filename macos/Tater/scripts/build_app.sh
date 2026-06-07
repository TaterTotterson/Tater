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
  --exclude='.git/' \
  --exclude='.github/' \
  --exclude='.agents/' \
  --exclude='.codex/' \
  --exclude='.venv/' \
  --exclude='.runtime/' \
  --exclude='agent_lab/' \
  --exclude='cores/' \
  --exclude='integrations/' \
  --exclude='macos/' \
  --exclude='portals/' \
  --exclude='verba/' \
  --exclude='wav2vec2_checkpoints/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  --exclude='.DS_Store' \
  "${REPO_ROOT}/" "${SOURCE_SNAPSHOT_DIR}/"

chmod +x "${MACOS_DIR}/TaterAssistant"

printf 'Built %s\n' "${APP_DIR}"
