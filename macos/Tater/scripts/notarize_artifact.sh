#!/bin/sh
set -eu

ARTIFACT="${1:?Usage: notarize_artifact.sh /path/to/artifact}"

if [ "${TATER_NOTARIZE:-0}" != "1" ]; then
  printf 'Skipping notarization for %s (set TATER_NOTARIZE=1 to enable).\n' "${ARTIFACT}"
  exit 0
fi

if [ ! -e "${ARTIFACT}" ]; then
  printf 'Cannot notarize missing artifact: %s\n' "${ARTIFACT}" >&2
  exit 1
fi

submission_output="$(mktemp "${TMPDIR:-/tmp}/tater-notary-submit.XXXXXX")"
trap 'rm -f "${submission_output}"' EXIT

notary_submission_id() {
  /usr/bin/python3 - "$submission_output" <<'PY' 2>/dev/null || true
import json
import sys

try:
    with open(sys.argv[1], "r", encoding="utf-8") as handle:
        data = json.load(handle)
except Exception:
    raise SystemExit(0)

submission_id = data.get("id") or data.get("submissionId")
if submission_id:
    print(submission_id)
PY
}

notary_submission_status() {
  /usr/bin/python3 - "$submission_output" <<'PY' 2>/dev/null || true
import json
import sys

try:
    with open(sys.argv[1], "r", encoding="utf-8") as handle:
        data = json.load(handle)
except Exception:
    raise SystemExit(0)

status = data.get("status")
if status:
    print(status)
PY
}

print_notary_log() {
  submission_id="$1"
  if [ -z "${submission_id}" ]; then
    return
  fi
  printf '\nApple notary log for %s:\n' "${submission_id}" >&2
  if [ -n "${TATER_NOTARY_PROFILE:-}" ]; then
    xcrun notarytool log "${submission_id}" --keychain-profile "${TATER_NOTARY_PROFILE}" >&2 || true
  elif [ -n "${APPLE_API_KEY_PATH:-}" ] && [ -n "${APPLE_API_KEY_ID:-}" ] && [ -n "${APPLE_API_ISSUER_ID:-}" ]; then
    xcrun notarytool log \
      "${submission_id}" \
      --key "${APPLE_API_KEY_PATH}" \
      --key-id "${APPLE_API_KEY_ID}" \
      --issuer "${APPLE_API_ISSUER_ID}" >&2 || true
  elif [ -n "${APPLE_ID:-}" ] && [ -n "${APPLE_TEAM_ID:-}" ] && [ -n "${APPLE_APP_SPECIFIC_PASSWORD:-}" ]; then
    xcrun notarytool log \
      "${submission_id}" \
      --apple-id "${APPLE_ID}" \
      --team-id "${APPLE_TEAM_ID}" \
      --password "${APPLE_APP_SPECIFIC_PASSWORD}" >&2 || true
  fi
}

submit_status=0
if [ -n "${TATER_NOTARY_PROFILE:-}" ]; then
  xcrun notarytool submit \
    "${ARTIFACT}" \
    --wait \
    --output-format json \
    --keychain-profile "${TATER_NOTARY_PROFILE}" > "${submission_output}" || submit_status="$?"
elif [ -n "${APPLE_API_KEY_PATH:-}" ] && [ -n "${APPLE_API_KEY_ID:-}" ] && [ -n "${APPLE_API_ISSUER_ID:-}" ]; then
  xcrun notarytool submit \
    "${ARTIFACT}" \
    --wait \
    --output-format json \
    --key "${APPLE_API_KEY_PATH}" \
    --key-id "${APPLE_API_KEY_ID}" \
    --issuer "${APPLE_API_ISSUER_ID}" > "${submission_output}" || submit_status="$?"
elif [ -n "${APPLE_ID:-}" ] && [ -n "${APPLE_TEAM_ID:-}" ] && [ -n "${APPLE_APP_SPECIFIC_PASSWORD:-}" ]; then
  xcrun notarytool submit \
    "${ARTIFACT}" \
    --wait \
    --output-format json \
    --apple-id "${APPLE_ID}" \
    --team-id "${APPLE_TEAM_ID}" \
    --password "${APPLE_APP_SPECIFIC_PASSWORD}" > "${submission_output}" || submit_status="$?"
else
  printf 'TATER_NOTARIZE=1, but no notarization credentials were configured.\n' >&2
  printf 'Set TATER_NOTARY_PROFILE, Apple API key env, or Apple ID/app-specific password env.\n' >&2
  exit 1
fi

cat "${submission_output}"
submission_status="$(notary_submission_status)"
if [ "${submit_status}" -ne 0 ] || [ "${submission_status}" = "Invalid" ]; then
  submission_id="$(notary_submission_id)"
  print_notary_log "${submission_id}"
  if [ "${submit_status}" -ne 0 ]; then
    exit "${submit_status}"
  fi
  exit 1
fi
