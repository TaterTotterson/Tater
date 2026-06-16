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

if [ -n "${TATER_NOTARY_PROFILE:-}" ]; then
  xcrun notarytool submit "${ARTIFACT}" --wait --keychain-profile "${TATER_NOTARY_PROFILE}"
elif [ -n "${APPLE_API_KEY_PATH:-}" ] && [ -n "${APPLE_API_KEY_ID:-}" ] && [ -n "${APPLE_API_ISSUER_ID:-}" ]; then
  xcrun notarytool submit \
    "${ARTIFACT}" \
    --wait \
    --key "${APPLE_API_KEY_PATH}" \
    --key-id "${APPLE_API_KEY_ID}" \
    --issuer "${APPLE_API_ISSUER_ID}"
elif [ -n "${APPLE_ID:-}" ] && [ -n "${APPLE_TEAM_ID:-}" ] && [ -n "${APPLE_APP_SPECIFIC_PASSWORD:-}" ]; then
  xcrun notarytool submit \
    "${ARTIFACT}" \
    --wait \
    --apple-id "${APPLE_ID}" \
    --team-id "${APPLE_TEAM_ID}" \
    --password "${APPLE_APP_SPECIFIC_PASSWORD}"
else
  printf 'TATER_NOTARIZE=1, but no notarization credentials were configured.\n' >&2
  printf 'Set TATER_NOTARY_PROFILE, Apple API key env, or Apple ID/app-specific password env.\n' >&2
  exit 1
fi
